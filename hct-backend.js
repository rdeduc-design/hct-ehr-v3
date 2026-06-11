/* ════════════════════════════════════════════════════════════════════════
   HCT EHR — Backend Integration Layer (v9)
   Healthcare and Technology Institute Inc. · "How Care Transforms"

   Loads AFTER the main app script and provides:
     1. Real authentication (Supabase email/password login + signup)
     2. Cloud persistence of all chart data (autosaved per user)
     3. Shared faculty data: student progress + chart submissions + grading
     4. Improved Faculty Dashboard (tabs, roster, grading, analytics)
     5. Mobile UX: off-canvas chart sidebar toggle
   If js/config.js is left empty the app runs in DEMO MODE (in-browser only).
   ════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  var CFG = window.HCT_CONFIG || {};
  var DEMO = !(CFG.SUPABASE_URL && CFG.SUPABASE_ANON_KEY);
  var sb = null;                 // supabase client
  var hctSession = null;         // supabase auth session
  var hctProfile = null;         // { id, full_name, role, student_no, email }
  var lastSavedState = null;     // JSON string of last persisted chart state
  var lastSyncedProgress = null; // JSON string of last persisted progress
  var saveTimer = null, progTimer = null;
  var saving = false;

  if (!DEMO) {
    try {
      sb = window.supabase.createClient(CFG.SUPABASE_URL, CFG.SUPABASE_ANON_KEY);
    } catch (e) {
      console.error('[HCT] Supabase init failed, falling back to demo mode:', e);
      DEMO = true;
    }
  }

  /* ──────────────────────────────────────────────────────────────────
     1. STATE PERSISTENCE
     Every mutable chart store in the app is snapshotted into one JSON
     document per user and upserted to `chart_states`.
     ────────────────────────────────────────────────────────────────── */
  var STATE_KEYS = [
    'WARD_ROOMS', 'PATIENTS', 'OPD_PATIENTS', 'LTC_PATIENTS',
    'ALLERGIES', 'MRN_MAP', 'VISIT_TYPE_MAP', 'SCR_ENTRIES', 'EHR_STORE',
    'MAR_MEDS', 'MAR_HISTORY', 'MAR_STATUS', 'MAR_PRN',
    'NOTES_STORE', 'SAMPLE_NOTES_LOADED', 'PROB_LIST_STORE',
    'ADM_INFO_STORE', 'HPI_STORE', 'PMSH_STORE', 'FORMS_STORE', 'FORM_DATA',
    'PANELS_STORE', 'BB_STORE', 'MICRO_STORE', 'IMAGING_STORE', 'IMG_UPLOAD_DATA',
    'SBAR_STORE', 'CAREPLAN_STORE', 'ALERT_STORE', 'SAVE_LOG',
    'FLOWSHEET_STORE', 'FSH_STORE', 'HOMEMEDS_STORE', 'OB_STORE',
    'IO_ENTRIES', 'ROS_ENTRIES', 'PE_ENTRIES', 'IMMUNIZATIONS',
    'LAB_DATA', 'VS_DATA', 'GLOBAL_ALERTS',
    'FACULTY_PROGRESS', 'SUBMITTED_CHARTS', 'DEBRIEF_SUBMITTED'
  ];

  function collectState() {
    var state = {};
    STATE_KEYS.forEach(function (k) {
      if (typeof window[k] !== 'undefined') {
        try { state[k] = window[k]; } catch (e) { /* ignore */ }
      }
    });
    return state;
  }

  function applyState(state) {
    if (!state) return;
    STATE_KEYS.forEach(function (k) {
      if (state[k] !== undefined && state[k] !== null) {
        window[k] = state[k];
      }
    });
    // Re-render directory views with restored data
    try { if (typeof init === 'function') init(); } catch (e) { console.warn('[HCT] re-render after restore:', e); }
  }

  function safeStringify(obj) {
    try { return JSON.stringify(obj); } catch (e) { console.warn('[HCT] state stringify failed', e); return null; }
  }

  async function saveChartState(force) {
    if (DEMO || !hctSession || saving) return;
    var str = safeStringify(collectState());
    if (!str) return;
    if (!force && str === lastSavedState) return; // nothing changed
    saving = true;
    try {
      var res = await sb.from('chart_states').upsert({
        user_id: hctSession.user.id,
        state: JSON.parse(str),
        updated_at: new Date().toISOString()
      }, { onConflict: 'user_id' });
      if (res.error) console.warn('[HCT] autosave error:', res.error.message);
      else { lastSavedState = str; setSyncBadge('saved'); }
    } catch (e) {
      console.warn('[HCT] autosave failed:', e);
    } finally { saving = false; }
  }

  async function loadChartState() {
    if (DEMO || !hctSession) return;
    try {
      var res = await sb.from('chart_states').select('state').eq('user_id', hctSession.user.id).maybeSingle();
      if (res.error) { console.warn('[HCT] load state error:', res.error.message); return; }
      if (res.data && res.data.state) {
        applyState(res.data.state);
        lastSavedState = safeStringify(collectState());
      }
    } catch (e) { console.warn('[HCT] load state failed:', e); }
  }

  // best-effort flush when the tab is hidden / closing
  function flushOnHide() {
    if (DEMO || !hctSession) return;
    var str = safeStringify(collectState());
    if (!str || str === lastSavedState) return;
    var body = JSON.stringify([{
      user_id: hctSession.user.id,
      state: JSON.parse(str),
      updated_at: new Date().toISOString()
    }]);
    try {
      fetch(CFG.SUPABASE_URL + '/rest/v1/chart_states?on_conflict=user_id', {
        method: 'POST',
        keepalive: body.length < 60000, // keepalive payload limit ~64KB
        headers: {
          'apikey': CFG.SUPABASE_ANON_KEY,
          'Authorization': 'Bearer ' + hctSession.access_token,
          'Content-Type': 'application/json',
          'Prefer': 'resolution=merge-duplicates'
        },
        body: body
      });
      lastSavedState = str;
    } catch (e) { /* best effort */ }
  }

  /* ──────────────────────────────────────────────────────────────────
     2. STUDENT PROGRESS + SUBMISSIONS SYNC (shared with faculty)
     ────────────────────────────────────────────────────────────────── */
  function findPxName(pxId) {
    var pools = [window.PATIENTS, window.OPD_PATIENTS, window.LTC_PATIENTS];
    for (var i = 0; i < pools.length; i++) {
      var pool = pools[i]; if (!pool) continue;
      var keys = Object.keys(pool);
      for (var j = 0; j < keys.length; j++) {
        var arr = pool[keys[j]]; if (!Array.isArray(arr)) continue;
        for (var k = 0; k < arr.length; k++) if (arr[k] && arr[k].id === pxId) return arr[k].name || pxId;
      }
    }
    return pxId;
  }

  async function syncProgress(force) {
    if (DEMO || !hctSession || !hctProfile) return;
    var FP = window.FACULTY_PROGRESS || {};
    var rows = [];
    Object.keys(FP).forEach(function (pxId) {
      var byStudent = FP[pxId] || {};
      Object.keys(byStudent).forEach(function (sId) {
        var prog = byStudent[sId] || {};
        Object.keys(prog).forEach(function (sec) {
          rows.push({
            user_id: hctSession.user.id,
            student_no: hctProfile.student_no || sId,
            student_name: hctProfile.full_name || 'Student Nurse',
            px_id: pxId,
            px_name: findPxName(pxId),
            section: sec,
            time_ms: Math.round(prog[sec].time_ms || 0),
            visits: prog[sec].visits || 0,
            last_activity: new Date().toISOString()
          });
        });
      });
    });
    if (!rows.length) return;
    var sig = safeStringify(rows.map(function (r) { return [r.px_id, r.section, r.time_ms, r.visits]; }));
    if (!force && sig === lastSyncedProgress) return;
    try {
      var res = await sb.from('student_progress').upsert(rows, { onConflict: 'user_id,px_id,section' });
      if (!res.error) lastSyncedProgress = sig;
      else console.warn('[HCT] progress sync error:', res.error.message);
    } catch (e) { console.warn('[HCT] progress sync failed:', e); }
  }

  async function pushSubmission(sub) {
    if (DEMO || !hctSession || !hctProfile || !sub) return;
    try {
      var res = await sb.from('submissions').insert({
        user_id: hctSession.user.id,
        student_no: hctProfile.student_no || sub.studentId,
        student_name: hctProfile.full_name || sub.studentName,
        px_id: sub.pxId,
        px_name: sub.pxName,
        answers: sub.answers || {},
        chart_snapshot: sub.chartSnapshot || {},
        completion: sub.completion || 0,
        submitted_at: sub.ts_iso || new Date().toISOString()
      });
      if (res.error) console.warn('[HCT] submission push error:', res.error.message);
    } catch (e) { console.warn('[HCT] submission push failed:', e); }
  }

  // Wrap the app's submitChart so submissions also reach the database
  var _origSubmitChart = window.submitChart;
  window.submitChart = function (pxId) {
    if (typeof _origSubmitChart === 'function') _origSubmitChart(pxId);
    try {
      var list = (window.SUBMITTED_CHARTS && window.SUBMITTED_CHARTS[pxId]) || [];
      var sub = list[list.length - 1];
      if (sub && hctProfile) {
        sub.studentName = hctProfile.full_name || sub.studentName;
        sub.studentId = hctProfile.student_no || sub.studentId;
      }
      pushSubmission(sub);
      syncProgress(true);
      saveChartState(true);
    } catch (e) { console.warn('[HCT] submit hook failed:', e); }
  };

  /* ──────────────────────────────────────────────────────────────────
     3. AUTHENTICATION
     ────────────────────────────────────────────────────────────────── */
  function $(id) { return document.getElementById(id); }
  function showErr(id, msg) { var el = $(id); if (el) { el.textContent = msg; el.style.display = msg ? 'block' : 'none'; } }
  function showInfo(id, msg) { var el = $(id); if (el) { el.textContent = msg; el.style.display = msg ? 'block' : 'none'; } }
  function setBusy(btnId, busy, label, busyLabel) {
    var b = $(btnId); if (!b) return;
    b.disabled = !!busy; b.style.opacity = busy ? '.6' : '1';
    b.textContent = busy ? busyLabel : label;
  }

  function applyUserToUI() {
    var name = hctProfile ? (hctProfile.full_name || 'User') : (window.curUserName || 'User');
    var roleKey = hctProfile ? hctProfile.role : (window.curUserRole || 'student');
    var isFaculty = roleKey === 'faculty' || roleKey === 'admin';
    window.curUserName = name;
    window.curUserRole = isFaculty ? 'faculty' : 'student';
    if (hctProfile && hctProfile.student_no) window.curStudentId = hctProfile.student_no;

    var roleLabel = roleKey === 'admin' ? 'Administrator' : (isFaculty ? 'Faculty / Instructor' : 'Student Nurse');
    var nameEl = document.querySelector('.tb-user div div:first-child');
    if (nameEl) nameEl.textContent = name;
    var roleEl = document.querySelector('.tb-user div div:last-child');
    if (roleEl) roleEl.textContent = roleLabel;
    var av = document.querySelector('.tb-avatar');
    if (av && name) av.textContent = name.split(' ').map(function (w) { return w[0]; }).join('').toUpperCase().substring(0, 2);
    var facBtn = $('nav-faculty-btn');
    if (facBtn) facBtn.style.display = isFaculty ? '' : 'none';
  }

  async function fetchOrCreateProfile(user) {
    var res = await sb.from('profiles').select('*').eq('id', user.id).maybeSingle();
    if (res.data) return res.data;
    // Profile missing (e.g. trigger not installed) — create from metadata
    var meta = user.user_metadata || {};
    var prof = {
      id: user.id,
      full_name: meta.full_name || (user.email || '').split('@')[0],
      role: meta.role || 'student',
      student_no: meta.student_no || ('S' + user.id.substring(0, 6).toUpperCase()),
      email: user.email
    };
    var ins = await sb.from('profiles').upsert(prof, { onConflict: 'id' });
    if (ins.error) console.warn('[HCT] profile create error:', ins.error.message);
    return prof;
  }

  async function enterApp() {
    applyUserToUI();
    await loadChartState();
    if (typeof showScreen === 'function') showScreen('s-app');
    startBackgroundSync();
    setSyncBadge('saved');
  }

  function startBackgroundSync() {
    if (saveTimer) clearInterval(saveTimer);
    if (progTimer) clearInterval(progTimer);
    if (DEMO) return;
    saveTimer = setInterval(function () { saveChartState(false); }, 15000);
    progTimer = setInterval(function () { syncProgress(false); }, 30000);
    document.addEventListener('visibilitychange', function () {
      if (document.visibilityState === 'hidden') flushOnHide();
    });
    window.addEventListener('beforeunload', flushOnHide);
  }

  var _origDoLogin = window.doLogin;
  window.doLogin = async function () {
    showErr('auth-error-login', '');
    if (DEMO) { if (typeof _origDoLogin === 'function') _origDoLogin(); return; }
    var email = ($('login-email') || {}).value || '';
    var pass = ($('login-password') || {}).value || '';
    email = email.trim();
    if (!email || !pass) { showErr('auth-error-login', 'Please enter your email and password.'); return; }
    setBusy('login-btn', true, 'Sign In', 'Signing in…');
    try {
      var res = await sb.auth.signInWithPassword({ email: email, password: pass });
      if (res.error) { showErr('auth-error-login', res.error.message); return; }
      hctSession = res.data.session;
      hctProfile = await fetchOrCreateProfile(res.data.user);
      await enterApp();
    } catch (e) {
      showErr('auth-error-login', 'Could not reach the server. Check your internet connection.');
    } finally {
      setBusy('login-btn', false, 'Sign In', 'Signing in…');
    }
  };

  window.doSignup = async function () {
    showErr('auth-error-signup', '');
    if (DEMO) { if (typeof _origDoLogin === 'function') _origDoLogin(); return; }
    var f = (($('reg-fname') || {}).value || '').trim();
    var l = (($('reg-lname') || {}).value || '').trim();
    var email = (($('reg-email') || {}).value || '').trim();
    var idno = (($('reg-idno') || {}).value || '').trim();
    var role = (($('reg-role') || {}).value || 'student');
    var p1 = ($('reg-password') || {}).value || '';
    var p2 = ($('reg-confirm') || {}).value || '';
    if (!f || !l) { showErr('auth-error-signup', 'Please enter your first and last name.'); return; }
    if (!email) { showErr('auth-error-signup', 'Please enter your email address.'); return; }
    if (p1.length < 6) { showErr('auth-error-signup', 'Password must be at least 6 characters.'); return; }
    if (p1 !== p2) { showErr('auth-error-signup', 'Passwords do not match.'); return; }
    setBusy('signup-btn', true, 'Create Account', 'Creating…');
    try {
      var res = await sb.auth.signUp({
        email: email, password: p1,
        options: { data: { full_name: f + ' ' + l, role: role, student_no: idno || null } }
      });
      if (res.error) { showErr('auth-error-signup', res.error.message); return; }
      if (res.data.session) {
        hctSession = res.data.session;
        hctProfile = await fetchOrCreateProfile(res.data.user);
        await enterApp();
      } else {
        // Email confirmation is enabled in Supabase
        if (typeof showScreen === 'function') showScreen('s-login');
        showInfo('auth-info-login', 'Account created! Check your email to confirm, then sign in.');
      }
    } catch (e) {
      showErr('auth-error-signup', 'Could not reach the server. Check your internet connection.');
    } finally {
      setBusy('signup-btn', false, 'Create Account', 'Creating…');
    }
  };

  window.hctLogout = async function () {
    try {
      if (!DEMO && hctSession) {
        await saveChartState(true);
        await syncProgress(true);
        await sb.auth.signOut();
      }
    } catch (e) { /* ignore */ }
    location.reload();
  };

  async function restoreSession() {
    if (DEMO) {
      var demoFields = $('login-demo-fields'); if (demoFields) demoFields.style.display = '';
      var demoNote = $('login-demo-note'); if (demoNote) demoNote.style.display = '';
      var pw = $('login-password'); if (pw) pw.closest('.fg').style.display = 'none';
      return;
    }
    try {
      var res = await sb.auth.getSession();
      if (res.data && res.data.session) {
        hctSession = res.data.session;
        hctProfile = await fetchOrCreateProfile(res.data.session.user);
        await enterApp();
      }
    } catch (e) { console.warn('[HCT] session restore failed:', e); }
  }

  /* ──────────────────────────────────────────────────────────────────
     4. SYNC STATUS BADGE (small, unobtrusive, in topbar)
     ────────────────────────────────────────────────────────────────── */
  function setSyncBadge(state) {
    if (DEMO) return;
    var el = $('hct-sync-badge');
    if (!el) {
      var bar = document.querySelector('.top-bar .tb-right');
      if (!bar) return;
      el = document.createElement('div');
      el.id = 'hct-sync-badge';
      el.style.cssText = 'font-size:10px;color:rgba(255,255,255,.55);display:flex;align-items:center;gap:4px;white-space:nowrap';
      bar.insertBefore(el, bar.firstChild);
    }
    el.innerHTML = state === 'saved'
      ? '<span style="width:7px;height:7px;border-radius:50%;background:#2BBFAD;display:inline-block"></span>Cloud sync on'
      : '<span style="width:7px;height:7px;border-radius:50%;background:#F59E0B;display:inline-block"></span>Saving…';
  }

  /* ──────────────────────────────────────────────────────────────────
     5. IMPROVED FACULTY DASHBOARD
     Tabs: Overview · Students · Submissions & Grading · Section Analytics
     Live data from Supabase (all students); in demo mode shows the
     current browser's in-memory data.
     ────────────────────────────────────────────────────────────────── */
  var SECTION_LABELS = {
    'visit-summary': 'Visit Summary', 'vitals': 'Vital Signs', 'mar': 'MAR',
    'notes': 'Nursing Notes', 'orders': 'Orders', 'labs': 'Labs', 'io': 'I&O',
    'screenings': 'Screenings', 'resp-assess': 'Resp Assessment',
    'physical-exam': 'Physical Exam', 'admission-info': 'Admission Info',
    'hpi': 'HPI', 'careplan': 'Care Plan', 'sbar': 'SBAR', 'imaging': 'Imaging',
    'immunizations': 'Immunizations', 'submit-chart': 'Submit & Debrief'
  };
  function secLabel(s) { return SECTION_LABELS[s] || s; }
  function esc(s) { return (typeof escHtml === 'function') ? escHtml(String(s == null ? '' : s)) : String(s == null ? '' : s); }
  function fmtMin(ms) { return (Math.round((ms || 0) / 6000) / 10) + ' min'; }
  function fmtDateShort(iso) {
    if (!iso) return '—';
    try { return new Date(iso).toLocaleString('en-PH', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }); }
    catch (e) { return iso; }
  }

  var _dashData = null; // {students, progress, submissions}

  async function fetchDashData() {
    if (DEMO || !sb) {
      // Build from in-memory stores (current browser only)
      var progress = [];
      var FP = window.FACULTY_PROGRESS || {};
      Object.keys(FP).forEach(function (pxId) {
        Object.keys(FP[pxId]).forEach(function (sId) {
          var prog = FP[pxId][sId];
          Object.keys(prog).forEach(function (sec) {
            progress.push({ student_no: sId, student_name: 'Student Nurse', px_id: pxId, px_name: findPxName(pxId), section: sec, time_ms: prog[sec].time_ms, visits: prog[sec].visits, last_activity: null });
          });
        });
      });
      var subs = [];
      var SC = window.SUBMITTED_CHARTS || {};
      Object.keys(SC).forEach(function (pxId) {
        (SC[pxId] || []).forEach(function (s, i) {
          subs.push({ id: 'demo-' + pxId + '-' + i, student_no: s.studentId, student_name: s.studentName, px_id: s.pxId, px_name: s.pxName, answers: s.answers, chart_snapshot: s.chartSnapshot, completion: s.completion || 0, submitted_at: s.ts_iso, grade: s.grade != null ? s.grade : null, feedback: s.feedback || '', _demo: s });
        });
      });
      var studentsMap = {};
      progress.concat(subs).forEach(function (r) { if (r.student_no) studentsMap[r.student_no] = { student_no: r.student_no, full_name: r.student_name, role: 'student' }; });
      return { students: Object.keys(studentsMap).map(function (k) { return studentsMap[k]; }), progress: progress, submissions: subs };
    }
    var pr = await Promise.all([
      sb.from('profiles').select('id, full_name, role, student_no, email, created_at').eq('role', 'student').order('full_name'),
      sb.from('student_progress').select('*').order('last_activity', { ascending: false }),
      sb.from('submissions').select('*').order('submitted_at', { ascending: false })
    ]);
    return {
      students: pr[0].data || [],
      progress: pr[1].data || [],
      submissions: pr[2].data || []
    };
  }

  window.showFacultyDashboard = async function () {
    if (window.curUserRole !== 'faculty') { alert('Faculty Dashboard is only accessible to faculty accounts.'); return; }
    var existing = $('faculty-dash-modal'); if (existing) existing.remove();
    var div = document.createElement('div');
    div.id = 'faculty-dash-modal';
    div.innerHTML =
      '<div class="fdash-overlay"><div class="fdash-modal">' +
      '<div class="fdash-hdr"><div class="fdash-title">Faculty Dashboard' + (DEMO ? ' <span class="fdash-demo-tag">DEMO — this browser only</span>' : '') + '</div>' +
      '<div style="display:flex;gap:8px;align-items:center"><button class="fdash-refresh" onclick="showFacultyDashboard()">↻ Refresh</button>' +
      '<button class="fdash-close" onclick="closeFacultyDash()">✕</button></div></div>' +
      '<div class="fdash-tabs">' +
      '<div class="fdash-tab active" data-tab="overview" onclick="fdashTab(\'overview\')">Overview</div>' +
      '<div class="fdash-tab" data-tab="students" onclick="fdashTab(\'students\')">Students</div>' +
      '<div class="fdash-tab" data-tab="subs" onclick="fdashTab(\'subs\')">Submissions & Grading</div>' +
      '<div class="fdash-tab" data-tab="analytics" onclick="fdashTab(\'analytics\')">Section Analytics</div>' +
      '</div>' +
      '<div class="fdash-body" id="fdash-body"><div class="fdash-loading">Loading live data…</div></div>' +
      '</div></div>';
    document.body.appendChild(div);
    try {
      _dashData = await fetchDashData();
      fdashTab('overview');
    } catch (e) {
      var b = $('fdash-body'); if (b) b.innerHTML = '<div class="fdash-loading">Could not load data: ' + esc(e.message || e) + '</div>';
    }
  };

  window.closeFacultyDash = function () {
    var el = $('faculty-dash-modal'); if (el) el.remove();
  };

  window.fdashTab = function (tab) {
    document.querySelectorAll('.fdash-tab').forEach(function (t) { t.classList.toggle('active', t.getAttribute('data-tab') === tab); });
    var body = $('fdash-body'); if (!body || !_dashData) return;
    if (tab === 'overview') body.innerHTML = renderOverview();
    else if (tab === 'students') body.innerHTML = renderStudents();
    else if (tab === 'subs') body.innerHTML = renderSubmissions();
    else if (tab === 'analytics') body.innerHTML = renderAnalytics();
  };

  function statCard(val, lbl, color) {
    return '<div class="fdash-stat" style="border-top-color:' + (color || 'var(--teal)') + '"><div class="fdash-stat-val">' + val + '</div><div class="fdash-stat-lbl">' + lbl + '</div></div>';
  }

  function renderOverview() {
    var d = _dashData;
    var pending = d.submissions.filter(function (s) { return s.grade == null; }).length;
    var graded = d.submissions.filter(function (s) { return s.grade != null; });
    var avgGrade = graded.length ? Math.round(graded.reduce(function (a, s) { return a + s.grade; }, 0) / graded.length) : null;
    var totalTime = d.progress.reduce(function (a, r) { return a + (r.time_ms || 0); }, 0);
    var today = new Date(); today.setHours(0, 0, 0, 0);
    var activeToday = {};
    d.progress.forEach(function (r) { if (r.last_activity && new Date(r.last_activity) >= today) activeToday[r.student_no] = 1; });

    var html = '<div class="fdash-stat-grid">' +
      statCard(d.students.length || Object.keys(groupBy(d.progress, 'student_no')).length, 'Registered Students') +
      statCard(Object.keys(activeToday).length, 'Active Today', '#16A34A') +
      statCard(d.submissions.length, 'Charts Submitted', '#2563EB') +
      statCard(pending, 'Pending Review', pending > 0 ? '#D97706' : '#16A34A') +
      statCard(avgGrade != null ? avgGrade + '%' : '—', 'Avg. Grade', '#7C3AED') +
      statCard(fmtMin(totalTime), 'Total Charting Time', '#0D9488') +
      '</div>';

    // Recent activity feed
    var recent = d.progress.slice(0, 8);
    html += '<div class="fdash-card"><div class="fdash-card-hdr">Recent Charting Activity</div>';
    if (!recent.length) html += '<div class="fdash-empty">No student activity recorded yet.</div>';
    else {
      html += '<div class="fdash-table-scroll"><table class="fdash-table"><thead><tr><th>Student</th><th>Patient</th><th>Section</th><th>Time</th><th>Visits</th><th>Last Active</th></tr></thead><tbody>';
      recent.forEach(function (r) {
        html += '<tr><td>' + esc(r.student_name) + ' <span class="fdash-muted">(' + esc(r.student_no) + ')</span></td><td>' + esc(r.px_name) + '</td><td>' + esc(secLabel(r.section)) + '</td><td>' + fmtMin(r.time_ms) + '</td><td>' + (r.visits || 0) + '</td><td>' + fmtDateShort(r.last_activity) + '</td></tr>';
      });
      html += '</tbody></table></div>';
    }
    html += '</div>';

    // Latest submissions preview
    var latest = d.submissions.slice(0, 3);
    if (latest.length) {
      html += '<div class="fdash-card"><div class="fdash-card-hdr">Latest Submissions</div>';
      latest.forEach(function (s) {
        html += '<div class="fdash-sub-line"><div><strong>' + esc(s.student_name) + '</strong> — ' + esc(s.px_name) + ' <span class="fdash-muted">· ' + fmtDateShort(s.submitted_at) + '</span></div>' +
          (s.grade != null ? '<span class="fdash-grade-pill graded">' + s.grade + '%</span>' : '<span class="fdash-grade-pill pending">Pending</span>') + '</div>';
      });
      html += '<div style="padding:10px 14px"><a class="fdash-link" onclick="fdashTab(\'subs\')">Review & grade all submissions →</a></div></div>';
    }
    return html;
  }

  function groupBy(rows, key) {
    var out = {};
    rows.forEach(function (r) { var k = r[key] || '—'; (out[k] = out[k] || []).push(r); });
    return out;
  }

  function renderStudents() {
    var d = _dashData;
    var byStudent = groupBy(d.progress, 'student_no');
    var subsByStudent = groupBy(d.submissions, 'student_no');
    var roster = d.students.length ? d.students : Object.keys(byStudent).map(function (k) { return { student_no: k, full_name: (byStudent[k][0] || {}).student_name || 'Student' }; });
    if (!roster.length) return '<div class="fdash-empty">No students registered yet. Students appear here after they sign up and begin charting.</div>';

    var html = '<div class="fdash-card"><div class="fdash-card-hdr">Student Roster (' + roster.length + ')</div><div class="fdash-table-scroll"><table class="fdash-table"><thead><tr><th>Student</th><th>ID No.</th><th>Patients Charted</th><th>Sections</th><th>Total Time</th><th>Submissions</th><th>Avg Grade</th><th>Last Active</th></tr></thead><tbody>';
    roster.forEach(function (st) {
      var rows = byStudent[st.student_no] || [];
      var subs = subsByStudent[st.student_no] || [];
      var graded = subs.filter(function (s) { return s.grade != null; });
      var avg = graded.length ? Math.round(graded.reduce(function (a, s) { return a + s.grade; }, 0) / graded.length) + '%' : '—';
      var pxSet = {}; rows.forEach(function (r) { pxSet[r.px_id] = 1; });
      var totalMs = rows.reduce(function (a, r) { return a + (r.time_ms || 0); }, 0);
      var last = rows.map(function (r) { return r.last_activity; }).filter(Boolean).sort().pop();
      html += '<tr><td><strong>' + esc(st.full_name) + '</strong></td><td class="fdash-muted">' + esc(st.student_no || '—') + '</td><td>' + Object.keys(pxSet).length + '</td><td>' + rows.length + '</td><td>' + fmtMin(totalMs) + '</td><td>' + subs.length + '</td><td>' + avg + '</td><td>' + fmtDateShort(last) + '</td></tr>';
      // expandable per-section detail row
      if (rows.length) {
        var detail = rows.map(function (r) { return esc(secLabel(r.section)) + ' (' + fmtMin(r.time_ms) + ', ' + r.visits + '×)'; }).join(' · ');
        html += '<tr class="fdash-detail-row"><td colspan="8">' + esc(st.full_name).split(' ')[0] + '\u2019s sections: ' + detail + '</td></tr>';
      }
    });
    html += '</tbody></table></div></div>';
    return html;
  }

  function renderSubmissions() {
    var d = _dashData;
    if (!d.submissions.length) return '<div class="fdash-empty">No charts submitted yet. Submissions appear here when students complete the PEARLS reflection and submit their chart.</div>';
    var PQ = window.PEARLS_QUESTIONS || [];
    var html = '';
    d.submissions.forEach(function (s) {
      var snap = s.chart_snapshot || {};
      var ansHtml = '';
      PQ.forEach(function (phase) {
        var answered = phase.questions.filter(function (q) { return s.answers && s.answers[q.id]; });
        if (!answered.length) return;
        ansHtml += '<div class="fdash-pearls-phase">' + esc(phase.icon || '') + ' ' + esc(phase.phase) + '</div>';
        answered.forEach(function (q) {
          ansHtml += '<div class="fdash-pearls-qa"><div class="fdash-pearls-q">' + esc(q.text) + '</div><div class="fdash-pearls-a">' + esc(s.answers[q.id]) + '</div></div>';
        });
      });
      var safeId = String(s.id).replace(/[^a-zA-Z0-9_-]/g, '');
      html += '<div class="fdash-card fdash-sub-card">' +
        '<div class="fdash-sub-hdr"><div><div class="fdash-sub-name">' + esc(s.student_name) + ' <span class="fdash-muted">(' + esc(s.student_no || '—') + ')</span></div>' +
        '<div class="fdash-muted">Patient: ' + esc(s.px_name) + ' · Submitted: ' + fmtDateShort(s.submitted_at) + ' · Chart completion: ' + (s.completion || 0) + '%</div></div>' +
        (s.grade != null ? '<span class="fdash-grade-pill graded">Graded: ' + s.grade + '%</span>' : '<span class="fdash-grade-pill pending">Pending Review</span>') + '</div>' +
        '<div class="fdash-muted" style="margin:6px 0 10px">Orders: ' + (snap.orders || 0) + ' · MAR meds: ' + (snap.meds || 0) + ' · BP entries: ' + (snap.bpEntries || 0) + '</div>' +
        '<details class="fdash-details"><summary>PEARLS Reflection</summary>' + (ansHtml || '<div class="fdash-muted" style="padding:8px 0">No reflection answers recorded.</div>') + '</details>' +
        '<div class="fdash-grade-row">' +
        '<input type="number" min="0" max="100" id="fdash-grade-' + safeId + '" value="' + (s.grade != null ? s.grade : '') + '" placeholder="Grade %" class="fdash-grade-input"/>' +
        '<input type="text" id="fdash-fb-' + safeId + '" value="' + esc(s.feedback || '') + '" placeholder="Feedback for the student…" class="fdash-fb-input"/>' +
        '<button class="fdash-grade-btn" onclick="fdashSaveGrade(\'' + esc(String(s.id)) + '\',\'' + safeId + '\')">Save Grade</button>' +
        '<span id="fdash-grade-msg-' + safeId + '" class="fdash-muted"></span>' +
        '</div></div>';
    });
    return html;
  }

  window.fdashSaveGrade = async function (subId, safeId) {
    var g = parseInt(($('fdash-grade-' + safeId) || {}).value, 10);
    var fb = (($('fdash-fb-' + safeId) || {}).value || '').trim();
    var msg = $('fdash-grade-msg-' + safeId);
    if (isNaN(g) || g < 0 || g > 100) { if (msg) msg.textContent = 'Enter a grade from 0–100.'; return; }
    if (msg) msg.textContent = 'Saving…';
    var sub = (_dashData.submissions || []).filter(function (s) { return String(s.id) === String(subId); })[0];
    if (DEMO) {
      if (sub) { sub.grade = g; sub.feedback = fb; if (sub._demo) { sub._demo.grade = g; sub._demo.feedback = fb; } }
      if (msg) msg.textContent = 'Saved (demo).';
      return;
    }
    try {
      var res = await sb.from('submissions').update({
        grade: g, feedback: fb,
        graded_by: hctProfile ? hctProfile.full_name : 'Faculty',
        graded_at: new Date().toISOString()
      }).eq('id', subId);
      if (res.error) { if (msg) msg.textContent = 'Error: ' + res.error.message; return; }
      if (sub) { sub.grade = g; sub.feedback = fb; }
      if (msg) msg.textContent = 'Saved ✓';
    } catch (e) { if (msg) msg.textContent = 'Save failed.'; }
  };

  function renderAnalytics() {
    var d = _dashData;
    if (!d.progress.length) return '<div class="fdash-empty">No charting activity yet — analytics will appear once students start working in patient charts.</div>';
    var totals = {};
    d.progress.forEach(function (r) {
      var t = totals[r.section] = totals[r.section] || { time: 0, visits: 0, students: {} };
      t.time += r.time_ms || 0; t.visits += r.visits || 0; t.students[r.student_no] = 1;
    });
    var secs = Object.keys(totals).sort(function (a, b) { return totals[b].time - totals[a].time; });
    var maxTime = totals[secs[0]].time || 1;
    var html = '<div class="fdash-card"><div class="fdash-card-hdr">Engagement by Chart Section</div><div style="padding:14px">';
    secs.forEach(function (sec) {
      var t = totals[sec];
      var pct = Math.max(2, Math.round(t.time / maxTime * 100));
      html += '<div class="fdash-bar-row"><div class="fdash-bar-label">' + esc(secLabel(sec)) + '</div>' +
        '<div class="fdash-bar-track"><div class="fdash-bar-fill" style="width:' + pct + '%"></div></div>' +
        '<div class="fdash-bar-val">' + fmtMin(t.time) + ' · ' + t.visits + ' visits · ' + Object.keys(t.students).length + ' student(s)</div></div>';
    });
    html += '</div></div>';

    // Least-engaged sections — teaching gap callout (retained & enhanced)
    var low = secs.slice().reverse().slice(0, 3);
    html += '<div class="fdash-card"><div class="fdash-card-hdr" style="background:#7A1F23">⚠ Areas Needing Improvement (least time spent)</div><div style="padding:14px">';
    low.forEach(function (sec) {
      html += '<div class="fdash-sub-line"><span style="font-weight:600;color:#B91C1C">' + esc(secLabel(sec)) + '</span><span class="fdash-muted">' + fmtMin(totals[sec].time) + ' total · ' + totals[sec].visits + ' visits</span></div>';
    });
    html += '<div class="fdash-muted" style="margin-top:10px">These sections received the least attention across the cohort. Consider targeted skills-lab activities, focused simulation objectives, or pre-briefing emphasis to close these documentation gaps before the next rotation.</div></div></div>';
    return html;
  }

  /* ──────────────────────────────────────────────────────────────────
     6. MOBILE: off-canvas chart sidebar toggle
     ────────────────────────────────────────────────────────────────── */
  function setupMobileNav() {
    var btn = document.createElement('button');
    btn.id = 'hct-mobile-nav-btn';
    btn.innerHTML = '☰ Chart Menu';
    btn.onclick = function () {
      var sbar = document.querySelector('.ehr-sidebar');
      var bd = $('hct-mobile-nav-backdrop');
      if (sbar) sbar.classList.toggle('mobile-open');
      if (bd) bd.classList.toggle('show', sbar && sbar.classList.contains('mobile-open'));
    };
    document.body.appendChild(btn);
    var backdrop = document.createElement('div');
    backdrop.id = 'hct-mobile-nav-backdrop';
    backdrop.onclick = closeMobileNav;
    document.body.appendChild(backdrop);
    // auto-close after choosing a section
    document.addEventListener('click', function (ev) {
      if (window.innerWidth > 768) return;
      var t = ev.target;
      if (t && (t.classList.contains('nav-child') || (t.classList.contains('nav-item') && !t.classList.contains('parent')))) {
        closeMobileNav();
      }
    });
  }
  function closeMobileNav() {
    var sbar = document.querySelector('.ehr-sidebar');
    var bd = $('hct-mobile-nav-backdrop');
    if (sbar) sbar.classList.remove('mobile-open');
    if (bd) bd.classList.remove('show');
  }

  /* ──────────────────────────────────────────────────────────────────
     7. DASHBOARD STYLES (injected)
     ────────────────────────────────────────────────────────────────── */
  function injectStyles() {
    var css = '' +
      '.fdash-overlay{position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:600;overflow-y:auto;padding:24px 12px}' +
      '.fdash-modal{background:var(--cream,#F8F7F3);max-width:1040px;margin:0 auto;border-radius:14px;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,.3)}' +
      '.fdash-hdr{background:var(--navy,#1B2A4A);padding:16px 20px;display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}' +
      '.fdash-title{color:#fff;font-family:"DM Serif Display",serif;font-size:20px}' +
      '.fdash-demo-tag{font-family:"DM Sans",sans-serif;font-size:10px;background:#D97706;color:#fff;padding:2px 8px;border-radius:9px;vertical-align:middle;margin-left:8px}' +
      '.fdash-close,.fdash-refresh{background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.25);color:#fff;border-radius:7px;padding:5px 12px;font-size:12px;cursor:pointer;font-family:"DM Sans",sans-serif}' +
      '.fdash-tabs{display:flex;background:#fff;border-bottom:1px solid var(--border,#E4E2DA);overflow-x:auto}' +
      '.fdash-tab{padding:11px 18px;font-size:13px;cursor:pointer;color:var(--text-muted,#6B7280);border-bottom:2.5px solid transparent;white-space:nowrap}' +
      '.fdash-tab.active{color:var(--teal-dark,#0D9488);border-bottom-color:var(--teal,#2BBFAD);font-weight:600}' +
      '.fdash-body{padding:18px}' +
      '.fdash-loading,.fdash-empty{text-align:center;padding:48px 20px;color:var(--text-muted,#6B7280);font-size:13px}' +
      '.fdash-stat-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px;margin-bottom:18px}' +
      '.fdash-stat{background:#fff;border:1px solid var(--border,#E4E2DA);border-radius:11px;padding:14px;border-top:3px solid var(--teal,#2BBFAD)}' +
      '.fdash-stat-val{font-size:26px;font-weight:700;color:var(--navy,#1B2A4A)}' +
      '.fdash-stat-lbl{font-size:10px;color:var(--text-muted,#6B7280);text-transform:uppercase;letter-spacing:.5px;font-weight:600;margin-top:2px}' +
      '.fdash-card{background:#fff;border:1px solid var(--border,#E4E2DA);border-radius:11px;margin-bottom:14px;overflow:hidden}' +
      '.fdash-card-hdr{background:var(--navy,#1B2A4A);color:#fff;padding:10px 14px;font-size:13px;font-weight:600}' +
      '.fdash-table-scroll{overflow-x:auto}' +
      '.fdash-table{width:100%;border-collapse:collapse;font-size:12px;min-width:640px}' +
      '.fdash-table th{padding:8px 12px;text-align:left;background:var(--cream,#F8F7F3);font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.4px;color:var(--text-muted,#6B7280);border-bottom:1px solid var(--border,#E4E2DA)}' +
      '.fdash-table td{padding:9px 12px;border-bottom:1px solid var(--border,#E4E2DA);vertical-align:middle;color:var(--text,#2A2E3A)}' +
      '.fdash-detail-row td{font-size:11px;color:var(--text-muted,#6B7280);background:var(--cream,#F8F7F3)}' +
      '.fdash-muted{color:var(--text-muted,#6B7280);font-size:11px;font-weight:400}' +
      '.fdash-sub-line{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:9px 14px;border-bottom:1px solid var(--border,#E4E2DA);font-size:12px;flex-wrap:wrap}' +
      '.fdash-grade-pill{font-size:10px;font-weight:700;padding:3px 9px;border-radius:10px;white-space:nowrap}' +
      '.fdash-grade-pill.graded{background:#DCFCE7;color:#16A34A}' +
      '.fdash-grade-pill.pending{background:#FEF3C7;color:#D97706}' +
      '.fdash-link{color:var(--teal-dark,#0D9488);font-size:12px;cursor:pointer;font-weight:600}' +
      '.fdash-sub-card{padding:14px}' +
      '.fdash-sub-hdr{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;flex-wrap:wrap}' +
      '.fdash-sub-name{font-size:13px;font-weight:700;color:var(--navy,#1B2A4A)}' +
      '.fdash-details{margin:8px 0;border:1px solid var(--border,#E4E2DA);border-radius:8px;padding:8px 12px;background:var(--cream,#F8F7F3)}' +
      '.fdash-details summary{font-size:12px;font-weight:600;color:var(--navy,#1B2A4A);cursor:pointer}' +
      '.fdash-pearls-phase{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.4px;color:var(--text-muted,#6B7280);margin:10px 0 4px}' +
      '.fdash-pearls-qa{padding:5px 0;border-bottom:1px solid var(--border,#E4E2DA)}' +
      '.fdash-pearls-q{font-size:11px;color:var(--text-muted,#6B7280)}' +
      '.fdash-pearls-a{font-size:12px;color:var(--navy,#1B2A4A);margin-top:2px;white-space:pre-wrap}' +
      '.fdash-grade-row{display:flex;gap:8px;align-items:center;margin-top:10px;flex-wrap:wrap}' +
      '.fdash-grade-input{width:90px;border:1.5px solid var(--border,#E4E2DA);border-radius:8px;padding:7px 10px;font-size:13px;font-family:"DM Sans",sans-serif}' +
      '.fdash-fb-input{flex:1;min-width:180px;border:1.5px solid var(--border,#E4E2DA);border-radius:8px;padding:7px 10px;font-size:13px;font-family:"DM Sans",sans-serif}' +
      '.fdash-grade-btn{background:var(--teal,#2BBFAD);border:none;color:#fff;border-radius:8px;padding:8px 16px;font-size:12px;font-weight:600;cursor:pointer;font-family:"DM Sans",sans-serif}' +
      '.fdash-bar-row{display:grid;grid-template-columns:150px 1fr auto;gap:10px;align-items:center;margin-bottom:8px}' +
      '.fdash-bar-label{font-size:12px;color:var(--navy,#1B2A4A);font-weight:500}' +
      '.fdash-bar-track{height:10px;background:var(--cream,#F8F7F3);border-radius:5px;border:1px solid var(--border,#E4E2DA);overflow:hidden}' +
      '.fdash-bar-fill{height:100%;background:linear-gradient(90deg,var(--teal,#2BBFAD),var(--navy,#1B2A4A));border-radius:5px}' +
      '.fdash-bar-val{font-size:11px;color:var(--text-muted,#6B7280);white-space:nowrap}' +
      '@media(max-width:640px){.fdash-bar-row{grid-template-columns:1fr;gap:4px}.fdash-body{padding:12px}}';
    var st = document.createElement('style');
    st.textContent = css;
    document.head.appendChild(st);
  }

  /* ──────────────────────────────────────────────────────────────────
     BOOT
     ────────────────────────────────────────────────────────────────── */
  function boot() {
    injectStyles();
    setupMobileNav();
    restoreSession();
    console.log('[HCT] Backend layer ready — mode: ' + (DEMO ? 'DEMO (no Supabase configured)' : 'CLOUD (Supabase)'));
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
