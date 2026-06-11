/* ════════════════════════════════════════════════════════════════════════
   HCT EHR — Backend Integration Layer (v12 — True Shared Real-Time)
   Healthcare and Technology Institute Inc. · "How Care Transforms"

   Architecture overview:
   ┌─────────────────────────────────────────────────────────────────────┐
   │  shared_ehr_state  (clinical data — ALL users share same rows)      │
   │  ehr_patients      (patient roster with soft-delete)                │
   │  ehr_notifications (persistent cross-user notification feed)        │
   │  chart_states      (per-user prefs/progress — backward compat)      │
   └─────────────────────────────────────────────────────────────────────┘

   Real-time flow:
   User A writes data → memory updated → hctFlushNow() →
     • WebSocket broadcast (instant, < 50 ms)          → User B/C receive
     • shared_ehr_state upsert (next 100 ms tick)      → postgres_changes
       fires on User B/C even if WS missed             → merge + re-render

   Patients:
   • Admitting  → ehr_patients INSERT + postgres_changes → all users
   • Discharging → is_discharged=true (soft delete)     → all users
   • Admin restore → is_discharged=false               → all users
   • Hard delete (admin) → deleted_at=now()            → hidden from all

   Notifications:
   • Every logSave() → ehr_notifications INSERT → postgres_changes
     fires on every connected client → badge increments + panel updates

   ════════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  var CFG  = window.HCT_CONFIG || {};
  var DEMO = !(CFG.SUPABASE_URL && CFG.SUPABASE_ANON_KEY);
  var sb   = null;
  var hctSession  = null;
  var hctProfile  = null;
  var lastSavedState      = null;
  var lastSyncedProgress  = null;
  var saving = false;

  var saveTimer        = null;
  var progTimer        = null;
  var pollTimer        = null;
  var broadcastTimer   = null;

  var liveChannel       = null;
  var dbChannel         = null;

  var lastBroadcastSnap = {};
  var lastSharedSnap    = {};
  var _patientsSeeded   = false;

  if (!DEMO) {
    try {
      sb = window.supabase.createClient(CFG.SUPABASE_URL, CFG.SUPABASE_ANON_KEY, {
        realtime: { params: { eventsPerSecond: 20 } }
      });
    } catch (e) {
      console.error('[HCT] Supabase init failed:', e);
      DEMO = true;
    }
  }

  /* ─────────────────────────────────────────────────────────────────────
     STORE CATALOGUES
     ───────────────────────────────────────────────────────────────────── */

  var PER_PATIENT_STORES = [
    'VS_DATA','NOTES_STORE','MAR_MEDS','MAR_HISTORY','MAR_STATUS','MAR_PRN',
    'PROB_LIST_STORE','ALLERGIES','ADM_INFO_STORE','HPI_STORE','PMSH_STORE',
    'CAREPLAN_STORE','SBAR_STORE','LAB_DATA','IMAGING_STORE','BB_STORE',
    'MICRO_STORE','PANELS_STORE','FLOWSHEET_STORE','FSH_STORE','IO_ENTRIES',
    'ROS_ENTRIES','PE_ENTRIES','IMMUNIZATIONS','FORMS_STORE','FORM_DATA',
    'OB_STORE','HOMEMEDS_STORE','SCR_ENTRIES','EHR_STORE','ALERT_STORE','SAVE_LOG'
  ];

  var PATIENT_LIST_STORES = ['PATIENTS','OPD_PATIENTS','LTC_PATIENTS'];
  var FLAT_STORES         = ['WARD_ROOMS','VISIT_TYPE_MAP','MRN_MAP'];
  var ARRAY_STORES        = ['GLOBAL_ALERTS'];

  var ALL_SHARED_STORES   = PER_PATIENT_STORES.concat(FLAT_STORES).concat(ARRAY_STORES);

  var STATE_KEYS = PER_PATIENT_STORES
    .concat(PATIENT_LIST_STORES).concat(FLAT_STORES).concat(ARRAY_STORES)
    .concat(['FACULTY_PROGRESS','SUBMITTED_CHARTS','DEBRIEF_SUBMITTED',
             'SAMPLE_NOTES_LOADED','IMG_UPLOAD_DATA']);

  var STORE_LABELS = {
    VS_DATA:'Vital Signs', NOTES_STORE:'Nursing Notes', MAR_MEDS:'Medication Orders',
    MAR_HISTORY:'Medication Administration', MAR_STATUS:'MAR Status',
    MAR_PRN:'PRN Medications', PROB_LIST_STORE:'Problem List / Diagnosis',
    ALLERGIES:'Allergies', ADM_INFO_STORE:'Admission Information',
    HPI_STORE:'History of Present Illness', PMSH_STORE:'Past Medical History',
    CAREPLAN_STORE:'Care Plan', SBAR_STORE:'SBAR Communication',
    LAB_DATA:'Laboratory Results', IMAGING_STORE:'Imaging / Radiology',
    BB_STORE:'Blood Bank', MICRO_STORE:'Microbiology', FLOWSHEET_STORE:'Flowsheet',
    IO_ENTRIES:'Intake & Output', IMMUNIZATIONS:'Immunizations',
    EHR_STORE:'Chart Entry', SCR_ENTRIES:'Screening Results',
    ALERT_STORE:'Clinical Alert', GLOBAL_ALERTS:'Global Alert',
    PATIENTS:'Inpatient Admission', OPD_PATIENTS:'Outpatient Registration',
    LTC_PATIENTS:'LTC Resident Registration', WARD_ROOMS:'Room / Bed Assignment'
  };

  var NOTIFY_STORES = {
    VS_DATA:1, NOTES_STORE:1, MAR_MEDS:1, MAR_HISTORY:1, PROB_LIST_STORE:1,
    ALLERGIES:1, CAREPLAN_STORE:1, SBAR_STORE:1, LAB_DATA:1, IMAGING_STORE:1,
    SCR_ENTRIES:1, ALERT_STORE:1, GLOBAL_ALERTS:1,
    PATIENTS:1, OPD_PATIENTS:1, LTC_PATIENTS:1
  };

  var ALERT_TRIGGER_STORES = { VS_DATA:1, SCR_ENTRIES:1 };

  /* ─────────────────────────────────────────────────────────────────────
     1. SHARED STATE — load / save / subscribe
     ───────────────────────────────────────────────────────────────────── */

  async function loadSharedState() {
    if (DEMO || !sb) return;
    try {
      var res = await sb.from('shared_ehr_state').select('state_key,px_id,data');
      if (res.error) { console.warn('[HCT] load shared state error:', res.error.message); return; }
      (res.data || []).forEach(function (row) {
        applySharedRow(row.state_key, row.px_id, row.data);
        try { lastSharedSnap[row.state_key + '|' + row.px_id] = JSON.stringify(row.data); } catch(e){}
      });
      try { if (typeof init === 'function') init(); } catch(e){}
    } catch(e) { console.warn('[HCT] load shared state failed:', e); }
  }

  function applySharedRow(key, pxId, data) {
    if (!key || data === undefined || data === null) return;
    if (pxId && pxId !== '__global' && PER_PATIENT_STORES.indexOf(key) >= 0) {
      if (!window[key]) window[key] = {};
      window[key][pxId] = data;
    } else if (FLAT_STORES.indexOf(key) >= 0) {
      window[key] = Object.assign({}, window[key] || {}, data);
    } else if (ARRAY_STORES.indexOf(key) >= 0 && Array.isArray(data)) {
      var local = window[key] || [];
      var localKeys = local.map(function(a){ return (a.msg||'')+'|'+(a.pxId||''); });
      var newOnes = data.filter(function(a){
        return localKeys.indexOf((a.msg||'')+'|'+(a.pxId||'')) < 0;
      });
      if (newOnes.length) window[key] = local.concat(newOnes);
    }
  }

  async function saveSharedState() {
    if (DEMO || !sb || !hctSession) return;
    var rows = [];
    var now  = new Date().toISOString();
    var uid  = hctSession.user.id;
    var uname = hctProfile ? (hctProfile.full_name || 'User') : 'User';

    PER_PATIENT_STORES.forEach(function(key) {
      var store = window[key];
      if (!store || typeof store !== 'object') return;
      Object.keys(store).forEach(function(pxId) {
        try {
          var curr = JSON.stringify(store[pxId]);
          var snapKey = key + '|' + pxId;
          if (curr === lastSharedSnap[snapKey]) return;
          rows.push({ state_key: key, px_id: pxId, data: store[pxId],
                      updated_by: uid, updated_by_name: uname, updated_at: now });
          lastSharedSnap[snapKey] = curr;
        } catch(e){}
      });
    });

    FLAT_STORES.forEach(function(key) {
      var store = window[key];
      if (!store) return;
      try {
        var curr = JSON.stringify(store);
        var snapKey = key + '|__global';
        if (curr === lastSharedSnap[snapKey]) return;
        rows.push({ state_key: key, px_id: '__global', data: store,
                    updated_by: uid, updated_by_name: uname, updated_at: now });
        lastSharedSnap[snapKey] = curr;
      } catch(e){}
    });

    ARRAY_STORES.forEach(function(key) {
      var store = window[key];
      if (!Array.isArray(store) || !store.length) return;
      try {
        var curr = JSON.stringify(store);
        var snapKey = key + '|__global';
        if (curr === lastSharedSnap[snapKey]) return;
        rows.push({ state_key: key, px_id: '__global', data: store,
                    updated_by: uid, updated_by_name: uname, updated_at: now });
        lastSharedSnap[snapKey] = curr;
      } catch(e){}
    });

    if (!rows.length) return;
    try {
      var chunks = [];
      for (var i = 0; i < rows.length; i += 50) chunks.push(rows.slice(i, i + 50));
      for (var c = 0; c < chunks.length; c++) {
        var res = await sb.from('shared_ehr_state').upsert(chunks[c],
          { onConflict: 'state_key,px_id' });
        if (res.error) console.warn('[HCT] shared state save error:', res.error.message);
      }
    } catch(e) { console.warn('[HCT] shared state save failed:', e); }
  }

  /* ─────────────────────────────────────────────────────────────────────
     2. PATIENT TABLE — load / admit / update / discharge / restore
     ───────────────────────────────────────────────────────────────────── */

  async function loadPatients() {
    if (DEMO || !sb) return;
    try {
      var res = await sb.from('ehr_patients')
        .select('*')
        .is('deleted_at', null)
        .eq('is_discharged', false)
        .order('admitted_at', { ascending: true });
      if (res.error) { console.warn('[HCT] load patients error:', res.error.message); return; }
      var rows = res.data || [];
      if (!rows.length) {
        await seedDefaultPatients();
        return;
      }
      applyPatientRows(rows);
    } catch(e) { console.warn('[HCT] load patients failed:', e); }
  }

  function applyPatientRows(rows) {
    var newPATIENTS     = {};
    var newOPD          = {};
    var newLTC          = {};
    rows.forEach(function(row) {
      var px = dbRowToPx(row);
      if (row.section_type === 'outpatient') {
        if (!newOPD[row.ward]) newOPD[row.ward] = [];
        newOPD[row.ward].push(px);
      } else if (row.section_type === 'ltc') {
        if (!newLTC[row.ward]) newLTC[row.ward] = [];
        newLTC[row.ward].push(px);
      } else {
        if (!newPATIENTS[row.ward]) newPATIENTS[row.ward] = [];
        newPATIENTS[row.ward].push(px);
      }
    });
    mergePxStore(window.PATIENTS,     newPATIENTS);
    mergePxStore(window.OPD_PATIENTS, newOPD);
    mergePxStore(window.LTC_PATIENTS, newLTC);
  }

  function mergePxStore(dest, incoming) {
    Object.keys(incoming).forEach(function(ward) {
      if (!dest[ward]) dest[ward] = [];
      var idMap = {};
      dest[ward].forEach(function(p, i) { idMap[p.id] = i; });
      incoming[ward].forEach(function(p) {
        if (idMap[p.id] !== undefined) dest[ward][idMap[p.id]] = p;
        else { dest[ward].push(p); idMap[p.id] = dest[ward].length - 1; }
      });
    });
  }

  function dbRowToPx(row) {
    return {
      id: row.id, name: row.name, age: row.age, sex: row.sex,
      room: row.room, dx: row.dx, status: row.status,
      physician: row.physician, admitted: row.admitted, dob: row.dob,
      allergies: row.allergies || [], photo: row.photo,
      _ward: row.ward, _sectionType: row.section_type
    };
  }

  function pxToDbRow(px, sectionType, wardId) {
    return {
      id: px.id, mrn: px.mrn || null,
      name: px.name, age: px.age || null, sex: px.sex || null,
      room: px.room || null, ward: wardId || px._ward || 'unknown',
      section_type: sectionType || px._sectionType || 'inpatient',
      dx: px.dx || null, status: px.status || 'admitted',
      physician: px.physician || null, admitted: px.admitted || null,
      dob: px.dob || null, allergies: px.allergies || [],
      photo: px.photo || null, extra: {},
      updated_by: hctSession ? hctSession.user.id : null,
      updated_by_name: hctProfile ? (hctProfile.full_name || 'User') : 'User',
      updated_at: new Date().toISOString()
    };
  }

  async function seedDefaultPatients() {
    if (_patientsSeeded || DEMO || !sb || !hctSession) return;
    _patientsSeeded = true;
    var rows = [];
    ['PATIENTS','OPD_PATIENTS','LTC_PATIENTS'].forEach(function(storeName) {
      var store = window[storeName];
      var stype = storeName === 'PATIENTS' ? 'inpatient'
                : storeName === 'OPD_PATIENTS' ? 'outpatient' : 'ltc';
      if (!store) return;
      Object.keys(store).forEach(function(ward) {
        (store[ward] || []).forEach(function(px) {
          rows.push(Object.assign(pxToDbRow(px, stype, ward), {
            created_by: hctSession.user.id
          }));
        });
      });
    });
    if (!rows.length) return;
    try {
      var res = await sb.from('ehr_patients').upsert(rows, { onConflict: 'id', ignoreDuplicates: true });
      if (res.error) console.warn('[HCT] seed patients error:', res.error.message);
    } catch(e) { console.warn('[HCT] seed patients failed:', e); }
  }

  window.hctAdmitPatient = async function(px, sectionType, wardId) {
    if (DEMO || !sb || !hctSession) return;
    try {
      var row = Object.assign(pxToDbRow(px, sectionType, wardId), {
        created_by: hctSession.user.id, admitted_at: new Date().toISOString()
      });
      var res = await sb.from('ehr_patients').upsert(row, { onConflict: 'id' });
      if (res.error) console.warn('[HCT] admit patient error:', res.error.message);
    } catch(e) { console.warn('[HCT] admit patient failed:', e); }
  };

  window.hctUpdatePatient = async function(px, sectionType, wardId) {
    if (DEMO || !sb || !hctSession) return;
    try {
      var row = pxToDbRow(px, sectionType, wardId);
      var res = await sb.from('ehr_patients').update(row).eq('id', px.id);
      if (res.error) console.warn('[HCT] update patient error:', res.error.message);
    } catch(e) { console.warn('[HCT] update patient failed:', e); }
  };

  window.hctDischargePatient = async function(pxId) {
    if (DEMO || !sb || !hctSession) return;
    try {
      var res = await sb.from('ehr_patients').update({
        is_discharged: true,
        discharge_date: new Date().toISOString(),
        updated_by: hctSession.user.id,
        updated_at: new Date().toISOString()
      }).eq('id', pxId);
      if (res.error) console.warn('[HCT] discharge patient error:', res.error.message);
      else await window.hctPushNotification('Patient discharged', 'info', pxId, 'Discharge');
    } catch(e) { console.warn('[HCT] discharge patient failed:', e); }
  };

  window.hctRestorePatient = async function(pxId) {
    if (DEMO || !sb || !hctSession) return;
    var isAdmin = hctProfile && hctProfile.role === 'admin';
    if (!isAdmin) { alert('Only administrators can restore discharged patients.'); return; }
    try {
      var res = await sb.from('ehr_patients').update({
        is_discharged: false,
        discharge_date: null,
        deleted_at: null,
        deleted_by: null,
        updated_by: hctSession.user.id,
        updated_at: new Date().toISOString()
      }).eq('id', pxId);
      if (res.error) { console.warn('[HCT] restore patient error:', res.error.message); return; }
      var pxRes = await sb.from('ehr_patients').select('*').eq('id', pxId).single();
      if (pxRes.data) {
        var row = pxRes.data;
        var px  = dbRowToPx(row);
        var store = row.section_type === 'outpatient' ? window.OPD_PATIENTS
                  : row.section_type === 'ltc'        ? window.LTC_PATIENTS
                  : window.PATIENTS;
        if (!store[row.ward]) store[row.ward] = [];
        var idx = store[row.ward].findIndex(function(p){ return p.id === pxId; });
        if (idx >= 0) store[row.ward][idx] = px;
        else store[row.ward].push(px);
        try { if (typeof init === 'function') init(); } catch(e){}
        showLiveToast('Patient restored by admin', 'info');
        await window.hctPushNotification('Patient restored by admin', 'info', pxId, 'Restore');
      }
    } catch(e) { console.warn('[HCT] restore patient failed:', e); }
  };

  window.hctFetchDischargedPatients = async function() {
    if (DEMO || !sb) return [];
    try {
      var res = await sb.from('ehr_patients')
        .select('*')
        .eq('is_discharged', true)
        .is('deleted_at', null)
        .order('discharge_date', { ascending: false });
      return res.data || [];
    } catch(e) { return []; }
  };

  /* ─────────────────────────────────────────────────────────────────────
     3. NOTIFICATIONS — load / push / subscribe / mark-read
     ───────────────────────────────────────────────────────────────────── */

  async function loadNotifications() {
    if (DEMO || !sb) return;
    try {
      var res = await sb.from('ehr_notifications')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(100);
      if (res.error) { console.warn('[HCT] load notifications error:', res.error.message); return; }
      (res.data || []).reverse().forEach(function(row) {
        injectNotifFromDB(row);
      });
      updateNotifBadge_safe();
      renderNotifList_safe();
    } catch(e) { console.warn('[HCT] load notifications failed:', e); }
  }

  function injectNotifFromDB(row) {
    if (!row || !row.id) return;
    var NOTIF_LOG = window.NOTIF_LOG;
    if (!Array.isArray(NOTIF_LOG)) { window.NOTIF_LOG = []; NOTIF_LOG = window.NOTIF_LOG; }
    var existing = NOTIF_LOG.find(function(n){ return n._dbId === row.id; });
    if (existing) return;
    var uid = hctSession ? hctSession.user.id : null;
    var readBy = Array.isArray(row.read_by) ? row.read_by : [];
    var isRead = uid ? readBy.indexOf(uid) >= 0 : false;
    window.NOTIF_ID_CTR = (window.NOTIF_ID_CTR || 0) + 1;
    NOTIF_LOG.unshift({
      id:      window.NOTIF_ID_CTR,
      _dbId:   row.id,
      section: row.section || row.store_key || 'Update',
      summary: row.message,
      pxId:    row.px_id || null,
      ts:      fmtDateShort(row.created_at),
      read:    isRead,
      navKey:  row.section || row.store_key || 'vitals',
      _readBy: readBy
    });
  }

  window.hctPushNotification = async function(message, type, pxId, section) {
    if (DEMO || !sb || !hctSession) return;
    var pxName = pxId ? findPxName(pxId) : null;
    try {
      await sb.from('ehr_notifications').insert({
        notif_type:      type || 'info',
        section:         section || null,
        px_id:           pxId || null,
        px_name:         pxName || null,
        message:         message,
        created_by:      hctSession.user.id,
        created_by_name: hctProfile ? (hctProfile.full_name || 'User') : 'User',
        created_by_role: hctProfile ? (hctProfile.role || 'student') : 'student'
      });
    } catch(e) { console.warn('[HCT] push notification failed:', e); }
  };

  window.hctMarkNotifRead = async function(dbId) {
    if (DEMO || !sb || !hctSession || !dbId) return;
    var uid = hctSession.user.id;
    try {
      var cur = await sb.from('ehr_notifications').select('read_by').eq('id', dbId).single();
      if (cur.error) return;
      var readBy = Array.isArray(cur.data.read_by) ? cur.data.read_by : [];
      if (readBy.indexOf(uid) < 0) {
        readBy.push(uid);
        await sb.from('ehr_notifications').update({ read_by: readBy }).eq('id', dbId);
      }
    } catch(e){}
  };

  function updateNotifBadge_safe() {
    try { if (typeof updateNotifBadge === 'function') updateNotifBadge(); } catch(e){}
  }
  function renderNotifList_safe() {
    try { if (typeof renderNotifList === 'function') renderNotifList(); } catch(e){}
  }

  /* ─────────────────────────────────────────────────────────────────────
     4. LEGACY PERSISTENCE — chart_states (per-user prefs/progress)
     ───────────────────────────────────────────────────────────────────── */

  function collectUserPrefs() {
    return {
      FACULTY_PROGRESS:    window.FACULTY_PROGRESS    || {},
      SUBMITTED_CHARTS:    window.SUBMITTED_CHARTS    || {},
      DEBRIEF_SUBMITTED:   window.DEBRIEF_SUBMITTED   || {},
      SAMPLE_NOTES_LOADED: window.SAMPLE_NOTES_LOADED || false,
      IMG_UPLOAD_DATA:     window.IMG_UPLOAD_DATA     || {}
    };
  }

  async function saveUserPrefs() {
    if (DEMO || !hctSession || saving) return;
    var str = safeStr(collectUserPrefs());
    if (!str || str === lastSavedState) return;
    saving = true;
    try {
      var res = await sb.from('chart_states').upsert({
        user_id: hctSession.user.id,
        state: JSON.parse(str),
        updated_at: new Date().toISOString()
      }, { onConflict: 'user_id' });
      if (res.error) console.warn('[HCT] prefs save error:', res.error.message);
      else lastSavedState = str;
    } catch(e) { console.warn('[HCT] prefs save failed:', e); }
    finally { saving = false; }
  }

  async function loadUserPrefs() {
    if (DEMO || !hctSession || !sb) return;
    try {
      var res = await sb.from('chart_states')
        .select('state').eq('user_id', hctSession.user.id).maybeSingle();
      if (res.error || !res.data || !res.data.state) return;
      var s = res.data.state;
      ['FACULTY_PROGRESS','SUBMITTED_CHARTS','DEBRIEF_SUBMITTED',
       'SAMPLE_NOTES_LOADED','IMG_UPLOAD_DATA'].forEach(function(k) {
        if (s[k] !== undefined) window[k] = s[k];
      });
    } catch(e) { console.warn('[HCT] load user prefs failed:', e); }
  }

  function safeStr(obj) {
    try { return JSON.stringify(obj); } catch(e) { return null; }
  }

  function flushOnHide() {
    if (DEMO || !hctSession) return;
    var str = safeStr(collectUserPrefs());
    if (!str || str === lastSavedState) return;
    try {
      fetch(CFG.SUPABASE_URL + '/rest/v1/chart_states?on_conflict=user_id', {
        method: 'POST', keepalive: str.length < 60000,
        headers: {
          'apikey': CFG.SUPABASE_ANON_KEY,
          'Authorization': 'Bearer ' + hctSession.access_token,
          'Content-Type': 'application/json',
          'Prefer': 'resolution=merge-duplicates'
        },
        body: JSON.stringify([{
          user_id: hctSession.user.id,
          state: JSON.parse(str),
          updated_at: new Date().toISOString()
        }])
      });
      lastSavedState = str;
    } catch(e){}
  }

  /* ─────────────────────────────────────────────────────────────────────
     5. TRUE REAL-TIME BROADCAST — 100 ms diff cycle
     ───────────────────────────────────────────────────────────────────── */

  function initBroadcastSnap() {
    PER_PATIENT_STORES.forEach(function(key) {
      lastBroadcastSnap[key] = {};
      var store = window[key];
      if (!store || typeof store !== 'object') return;
      Object.keys(store).forEach(function(pxId) {
        try { lastBroadcastSnap[key][pxId] = JSON.stringify(store[pxId]); } catch(e){}
      });
    });
    PATIENT_LIST_STORES.concat(FLAT_STORES).concat(ARRAY_STORES).forEach(function(key) {
      try { lastBroadcastSnap['__' + key] = JSON.stringify(window[key]); } catch(e){}
    });
    PER_PATIENT_STORES.forEach(function(key) {
      var store = window[key];
      if (!store) return;
      Object.keys(store).forEach(function(pxId) {
        try { lastSharedSnap[key + '|' + pxId] = JSON.stringify(store[pxId]); } catch(e){}
      });
    });
    FLAT_STORES.concat(ARRAY_STORES).forEach(function(key) {
      try { lastSharedSnap[key + '|__global'] = JSON.stringify(window[key]); } catch(e){}
    });
  }

  function safeSend(payload) {
    if (!liveChannel) return false;
    try {
      var str = JSON.stringify(payload);
      if (str.length > 28000) return false;
      liveChannel.send({ type: 'broadcast', event: 'chart_change', payload: payload });
      return true;
    } catch(e) { return false; }
  }

  function mkPayload(storeKey, pxId, data) {
    return {
      u:  hctSession.user.id,
      un: hctProfile ? (hctProfile.full_name || 'User') : 'User',
      ur: hctProfile ? (hctProfile.role || 'student') : 'student',
      k:  storeKey, px: pxId || null, d: data, t: Date.now()
    };
  }

  function broadcastChanges() {
    if (DEMO || !liveChannel || !hctSession || !hctProfile) return;
    var hadChanges = false;

    PER_PATIENT_STORES.forEach(function(key) {
      var store = window[key];
      if (!store || typeof store !== 'object') return;
      if (!lastBroadcastSnap[key]) lastBroadcastSnap[key] = {};
      Object.keys(store).forEach(function(pxId) {
        try {
          var curr = JSON.stringify(store[pxId]);
          if (curr === lastBroadcastSnap[key][pxId]) return;
          if (safeSend(mkPayload(key, pxId, store[pxId]))) {
            lastBroadcastSnap[key][pxId] = curr;
            hadChanges = true;
          }
        } catch(e){}
      });
    });

    PATIENT_LIST_STORES.forEach(function(key) {
      var store = window[key];
      if (!store) return;
      try {
        var curr = JSON.stringify(store);
        if (curr === lastBroadcastSnap['__' + key]) return;
        if (safeSend(mkPayload(key, null, store))) {
          lastBroadcastSnap['__' + key] = curr;
          hadChanges = true;
        }
      } catch(e){}
    });

    FLAT_STORES.forEach(function(key) {
      var store = window[key];
      if (!store) return;
      try {
        var curr = JSON.stringify(store);
        if (curr === lastBroadcastSnap['__' + key]) return;
        if (safeSend(mkPayload(key, null, store))) {
          lastBroadcastSnap['__' + key] = curr;
          hadChanges = true;
        }
      } catch(e){}
    });

    ARRAY_STORES.forEach(function(key) {
      var store = window[key];
      if (!Array.isArray(store) || !store.length) return;
      try {
        var curr = JSON.stringify(store);
        if (curr === lastBroadcastSnap['__' + key]) return;
        if (safeSend(mkPayload(key, null, store))) {
          lastBroadcastSnap['__' + key] = curr;
          hadChanges = true;
        }
      } catch(e){}
    });

    if (hadChanges) {
      setSyncBadge('saving');
      saveSharedState().then(function() { setSyncBadge('saved'); });
    }
  }

  window.hctFlushNow = function() {
    try { broadcastChanges(); } catch(e){}
  };

  function handleIncomingChange(p) {
    if (!p || !p.k) return;
    if (p.u === (hctSession && hctSession.user.id)) return;

    var key = p.k, pxId = p.px, data = p.d;
    var userName = p.un || 'Another user';
    if (data === undefined || data === null) return;

    var changed = false;
    var notifyPxName = '';

    if (pxId && PER_PATIENT_STORES.indexOf(key) >= 0) {
      var store = window[key] || {};
      store[pxId] = data;
      window[key] = store;
      if (!lastBroadcastSnap[key]) lastBroadcastSnap[key] = {};
      try { lastBroadcastSnap[key][pxId] = JSON.stringify(data); } catch(e){}
      try { lastSharedSnap[key + '|' + pxId] = JSON.stringify(data); } catch(e){}
      changed = true;
      notifyPxName = findPxName(pxId);
      if (ALERT_TRIGGER_STORES[key] && typeof checkAndFireAlerts === 'function') {
        try { checkAndFireAlerts(pxId); } catch(e){}
      }
    } else if (PATIENT_LIST_STORES.indexOf(key) >= 0) {
      var local = window[key] || {};
      Object.keys(data).forEach(function(ward) {
        var extArr = data[ward] || [];
        if (!extArr.length) return;
        if (!local[ward]) local[ward] = [];
        var idxMap = {};
        local[ward].forEach(function(p, i) { idxMap[p.id] = i; });
        extArr.forEach(function(ep) {
          if (idxMap[ep.id] === undefined) { local[ward].push(ep); }
          else local[ward][idxMap[ep.id]] = ep;
        });
      });
      window[key] = local;
      try { lastBroadcastSnap['__' + key] = JSON.stringify(local); } catch(e){}
      changed = true;
      var newest = null;
      Object.values(local).forEach(function(arr) {
        if (arr.length) newest = arr[arr.length - 1];
      });
      if (newest) notifyPxName = newest.name;
    } else if (FLAT_STORES.indexOf(key) >= 0) {
      window[key] = Object.assign({}, window[key] || {}, data);
      try { lastBroadcastSnap['__' + key] = JSON.stringify(window[key]); } catch(e){}
      changed = true;
    } else if (ARRAY_STORES.indexOf(key) >= 0 && Array.isArray(data)) {
      var localArr = window[key] || [];
      var localKeys = localArr.map(function(a){ return (a.msg||'')+'|'+(a.pxId||''); });
      var newOnes = data.filter(function(a){
        return localKeys.indexOf((a.msg||'')+'|'+(a.pxId||'')) < 0;
      });
      if (newOnes.length) {
        window[key] = localArr.concat(newOnes);
        try { lastBroadcastSnap['__' + key] = JSON.stringify(window[key]); } catch(e){}
        changed = true;
      }
    }

    if (!changed) return;
    try { if (typeof init === 'function') init(); } catch(e){}
    setSyncBadge('saved');

    if (NOTIFY_STORES[key]) {
      var label   = STORE_LABELS[key] || key;
      var isAlert = (key === 'ALERT_STORE' || key === 'GLOBAL_ALERTS');
      var roleTag = p.ur === 'faculty' ? 'Faculty' : (p.ur === 'admin' ? 'Admin' : 'Nurse');
      var who     = userName + ' (' + roleTag + ')';
      var msg;
      if (key === 'PATIENTS' || key === 'OPD_PATIENTS' || key === 'LTC_PATIENTS') {
        msg = notifyPxName ? who + ' admitted ' + notifyPxName : who + ' updated ' + label;
      } else {
        msg = who + ' updated ' + label + (notifyPxName ? ' for ' + notifyPxName : '');
      }
      showLiveToast(msg, isAlert ? 'alert' : 'info');
    }
  }

  /* ─────────────────────────────────────────────────────────────────────
     6. TOAST NOTIFICATION SYSTEM
     ───────────────────────────────────────────────────────────────────── */
  function showLiveToast(msg, type) {
    var container = document.getElementById('hct-toast-container');
    if (!container) {
      container = document.createElement('div');
      container.id = 'hct-toast-container';
      container.style.cssText =
        'position:fixed;bottom:18px;right:18px;z-index:99999;' +
        'display:flex;flex-direction:column-reverse;gap:8px;' +
        'pointer-events:none;max-width:320px;';
      document.body.appendChild(container);
    }
    while (container.children.length >= 4) {
      var oldest = container.lastChild;
      if (oldest) container.removeChild(oldest);
    }
    var isAlert = (type === 'alert');
    var bg      = isAlert ? '#7A1F23' : '#1B2A4A';
    var icon    = isAlert ? '⚠' : '⟳';
    var toast   = document.createElement('div');
    toast.style.cssText =
      'background:' + bg + ';color:#fff;padding:10px 13px;border-radius:10px;' +
      'font-family:"DM Sans",sans-serif;font-size:12px;line-height:1.45;' +
      'box-shadow:0 4px 18px rgba(0,0,0,.35);' +
      'display:flex;gap:9px;align-items:flex-start;' +
      'opacity:0;transform:translateX(18px);transition:opacity .22s,transform .22s;' +
      'pointer-events:auto;cursor:pointer;';
    toast.innerHTML =
      '<span style="font-size:14px;flex-shrink:0;margin-top:1px">' + icon + '</span>' +
      '<span>' + esc(msg) + '</span>';
    toast.onclick = function() { dismissToast(toast); };
    container.prepend(toast);
    requestAnimationFrame(function() {
      requestAnimationFrame(function() {
        toast.style.opacity   = '1';
        toast.style.transform = 'translateX(0)';
      });
    });
    setTimeout(function() { dismissToast(toast); }, isAlert ? 8000 : 5000);
  }

  function dismissToast(toast) {
    if (!toast || !toast.parentNode) return;
    toast.style.opacity   = '0';
    toast.style.transform = 'translateX(18px)';
    setTimeout(function() { if (toast.parentNode) toast.parentNode.removeChild(toast); }, 240);
  }

  /* ─────────────────────────────────────────────────────────────────────
     7. LIVE CHANNEL + POSTGRES_CHANGES SUBSCRIPTIONS
     ───────────────────────────────────────────────────────────────────── */

  function subscribeLiveChannel() {
    if (DEMO || !sb || !hctSession) return;
    try {
      if (liveChannel) { sb.removeChannel(liveChannel); liveChannel = null; }
      liveChannel = sb.channel('hct-ehr-live', {
        config: { broadcast: { self: false } }
      })
      .on('broadcast', { event: 'chart_change' }, function(msg) {
        handleIncomingChange(msg.payload);
      })
      .subscribe(function(status) {
        if (status === 'SUBSCRIBED') console.log('[HCT] Live channel connected');
        if (status === 'CHANNEL_ERROR') console.warn('[HCT] Live channel error');
      });
    } catch(e) { console.warn('[HCT] Live channel setup failed:', e); }
  }

  function subscribeDBChanges() {
    if (DEMO || !sb || !hctSession) return;
    try {
      if (dbChannel) { sb.removeChannel(dbChannel); dbChannel = null; }
      dbChannel = sb.channel('hct-ehr-db-v2')
        .on('postgres_changes', {
          event: 'INSERT', schema: 'public', table: 'shared_ehr_state'
        }, handleSharedStateChange)
        .on('postgres_changes', {
          event: 'UPDATE', schema: 'public', table: 'shared_ehr_state'
        }, handleSharedStateChange)
        .on('postgres_changes', {
          event: 'INSERT', schema: 'public', table: 'ehr_patients'
        }, handlePatientDBChange)
        .on('postgres_changes', {
          event: 'UPDATE', schema: 'public', table: 'ehr_patients'
        }, handlePatientDBChange)
        .on('postgres_changes', {
          event: 'INSERT', schema: 'public', table: 'ehr_notifications'
        }, handleNotificationDBChange)
        .subscribe(function(status) {
          if (status === 'SUBSCRIBED') console.log('[HCT] DB changes channel connected');
          if (status === 'CHANNEL_ERROR') console.warn('[HCT] DB changes error');
        });
    } catch(e) { console.warn('[HCT] DB changes setup failed:', e); }
  }

  function handleSharedStateChange(payload) {
    var row = payload.new || payload.record;
    if (!row) return;
    if (row.updated_by === (hctSession && hctSession.user.id)) return;
    applySharedRow(row.state_key, row.px_id, row.data);
    try { lastSharedSnap[row.state_key + '|' + row.px_id] = JSON.stringify(row.data); } catch(e){}
    try { if (typeof init === 'function') init(); } catch(e){}
    setSyncBadge('saved');
    var label = STORE_LABELS[row.state_key] || row.state_key;
    var who   = row.updated_by_name || 'Another user';
    if (NOTIFY_STORES[row.state_key]) {
      var pxName = row.px_id && row.px_id !== '__global' ? findPxName(row.px_id) : '';
      showLiveToast(who + ' updated ' + label + (pxName ? ' for ' + pxName : ''), 'info');
      if (ALERT_TRIGGER_STORES[row.state_key] && row.px_id && row.px_id !== '__global') {
        try { if (typeof checkAndFireAlerts === 'function') checkAndFireAlerts(row.px_id); } catch(e){}
      }
    }
  }

  function handlePatientDBChange(payload) {
    var row = payload.new || payload.record;
    if (!row) return;
    var uid = hctSession && hctSession.user.id;

    if (row.is_discharged || row.deleted_at) {
      var store = row.section_type === 'outpatient' ? window.OPD_PATIENTS
                : row.section_type === 'ltc'        ? window.LTC_PATIENTS
                : window.PATIENTS;
      if (store && store[row.ward]) {
        store[row.ward] = store[row.ward].filter(function(p){ return p.id !== row.id; });
      }
      try { if (typeof init === 'function') init(); } catch(e){}
      if (row.updated_by !== uid) {
        showLiveToast(
          (row.updated_by_name || 'A user') + ' discharged patient ' + (row.name || ''),
          'info'
        );
      }
      return;
    }

    var px    = dbRowToPx(row);
    var store = row.section_type === 'outpatient' ? window.OPD_PATIENTS
              : row.section_type === 'ltc'        ? window.LTC_PATIENTS
              : window.PATIENTS;
    if (!store[row.ward]) store[row.ward] = [];
    var idx = store[row.ward].findIndex(function(p){ return p.id === row.id; });
    if (idx >= 0) store[row.ward][idx] = px;
    else store[row.ward].push(px);

    try { if (typeof init === 'function') init(); } catch(e){}
    setSyncBadge('saved');
    if (row.updated_by !== uid) {
      showLiveToast(
        (row.updated_by_name || 'A user') + ' updated patient ' + (row.name || ''),
        'info'
      );
    }
  }

  function handleNotificationDBChange(payload) {
    var row = payload.new || payload.record;
    if (!row) return;
    var uid = hctSession && hctSession.user.id;
    if (row.created_by === uid) return;
    injectNotifFromDB(row);
    updateNotifBadge_safe();
    renderNotifList_safe();
    var isAlert = (row.notif_type === 'alert' || row.notif_type === 'warning');
    showLiveToast(
      (row.created_by_name || 'A user') + ': ' + row.message,
      isAlert ? 'alert' : 'info'
    );
  }

  /* ─────────────────────────────────────────────────────────────────────
     8. STUDENT PROGRESS + SUBMISSIONS
     ───────────────────────────────────────────────────────────────────── */
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
    Object.keys(FP).forEach(function(pxId) {
      var byStudent = FP[pxId] || {};
      Object.keys(byStudent).forEach(function(sId) {
        var prog = byStudent[sId] || {};
        Object.keys(prog).forEach(function(sec) {
          rows.push({
            user_id: hctSession.user.id,
            student_no: hctProfile.student_no || sId,
            student_name: hctProfile.full_name || 'Student Nurse',
            px_id: pxId, px_name: findPxName(pxId), section: sec,
            time_ms: Math.round(prog[sec].time_ms || 0),
            visits: prog[sec].visits || 0,
            last_activity: new Date().toISOString()
          });
        });
      });
    });
    if (!rows.length) return;
    var sig = safeStr(rows.map(function(r){ return [r.px_id, r.section, r.time_ms, r.visits]; }));
    if (!force && sig === lastSyncedProgress) return;
    try {
      var res = await sb.from('student_progress').upsert(rows, { onConflict: 'user_id,px_id,section' });
      if (!res.error) lastSyncedProgress = sig;
      else console.warn('[HCT] progress sync error:', res.error.message);
    } catch(e) { console.warn('[HCT] progress sync failed:', e); }
  }

  async function pushSubmission(sub) {
    if (DEMO || !hctSession || !hctProfile || !sub) return;
    try {
      var res = await sb.from('submissions').insert({
        user_id: hctSession.user.id,
        student_no: hctProfile.student_no || sub.studentId,
        student_name: hctProfile.full_name || sub.studentName,
        px_id: sub.pxId, px_name: sub.pxName,
        answers: sub.answers || {}, chart_snapshot: sub.chartSnapshot || {},
        completion: sub.completion || 0,
        submitted_at: sub.ts_iso || new Date().toISOString()
      });
      if (res.error) console.warn('[HCT] submission push error:', res.error.message);
    } catch(e) { console.warn('[HCT] submission push failed:', e); }
  }

  var _origSubmitChart = window.submitChart;
  window.submitChart = function(pxId) {
    if (typeof _origSubmitChart === 'function') _origSubmitChart(pxId);
    try {
      var list = (window.SUBMITTED_CHARTS && window.SUBMITTED_CHARTS[pxId]) || [];
      var sub  = list[list.length - 1];
      if (sub && hctProfile) {
        sub.studentName = hctProfile.full_name || sub.studentName;
        sub.studentId   = hctProfile.student_no || sub.studentId;
      }
      pushSubmission(sub);
      syncProgress(true);
      saveUserPrefs();
    } catch(e) { console.warn('[HCT] submit hook failed:', e); }
  };

  /* ─────────────────────────────────────────────────────────────────────
     9. AUTHENTICATION
     ───────────────────────────────────────────────────────────────────── */
  function $(id) { return document.getElementById(id); }
  function showErr(id, msg)  { var el=$(id); if(el){el.textContent=msg; el.style.display=msg?'block':'none';} }
  function showInfo(id, msg) { var el=$(id); if(el){el.textContent=msg; el.style.display=msg?'block':'none';} }
  function setBusy(btnId, busy, label, busyLabel) {
    var b=$(btnId); if(!b) return;
    b.disabled=!!busy; b.style.opacity=busy?'.6':'1';
    b.textContent=busy?busyLabel:label;
  }

  function applyUserToUI() {
    var name     = hctProfile ? (hctProfile.full_name||'User') : (window.curUserName||'User');
    var roleKey  = hctProfile ? hctProfile.role : (window.curUserRole||'student');
    var isFaculty = roleKey==='faculty'||roleKey==='admin';
    var isAdmin   = roleKey==='admin';
    window.curUserName  = name;
    window.curUserRole  = isFaculty ? 'faculty' : 'student';
    window.curIsAdmin   = isAdmin;
    if (hctProfile && hctProfile.student_no) window.curStudentId = hctProfile.student_no;
    var roleLabel = roleKey==='admin' ? 'Administrator'
                  : isFaculty ? 'Faculty / Instructor' : 'Student Nurse';
    var nameEl = document.querySelector('.tb-user div div:first-child');
    if (nameEl) nameEl.textContent = name;
    var roleEl = document.querySelector('.tb-user div div:last-child');
    if (roleEl) roleEl.textContent = roleLabel;
    var av = document.querySelector('.tb-avatar');
    if (av && name) av.textContent = name.split(' ').map(function(w){return w[0];}).join('').toUpperCase().substring(0,2);
    var facBtn = $('nav-faculty-btn');
    if (facBtn) facBtn.style.display = isFaculty ? '' : 'none';
    var adminBtn = $('nav-admin-btn');
    if (adminBtn) adminBtn.style.display = isAdmin ? '' : 'none';
  }

  async function fetchOrCreateProfile(user) {
    var res = await sb.from('profiles').select('*').eq('id', user.id).maybeSingle();
    if (res.data) return res.data;
    var meta = user.user_metadata || {};
    var prof = {
      id: user.id,
      full_name: meta.full_name || (user.email||'').split('@')[0],
      role: meta.role || 'student',
      student_no: meta.student_no || ('S'+user.id.substring(0,6).toUpperCase()),
      email: user.email
    };
    var ins = await sb.from('profiles').upsert(prof, { onConflict: 'id' });
    if (ins.error) console.warn('[HCT] profile create error:', ins.error.message);
    return prof;
  }

  async function enterApp() {
    applyUserToUI();
    setSyncBadge('saving');
    await loadPatients();
    await loadSharedState();
    await loadNotifications();
    await loadUserPrefs();
    if (typeof showScreen === 'function') showScreen('s-app');
    initBroadcastSnap();
    startBackgroundSync();
    setSyncBadge('saved');
  }

  function startBackgroundSync() {
    if (saveTimer)      clearInterval(saveTimer);
    if (progTimer)      clearInterval(progTimer);
    if (pollTimer)      clearInterval(pollTimer);
    if (broadcastTimer) clearInterval(broadcastTimer);
    if (DEMO) return;

    broadcastTimer = setInterval(function(){ broadcastChanges(); },   100);
    saveTimer      = setInterval(function(){ saveUserPrefs(); },     10000);
    progTimer      = setInterval(function(){ syncProgress(false); }, 30000);
    pollTimer      = setInterval(function(){ saveSharedState(); },   15000);

    document.addEventListener('visibilitychange', function() {
      if (document.visibilityState === 'hidden') { flushOnHide(); }
      if (document.visibilityState === 'visible') {
        saveUserPrefs();
        saveSharedState();
        initBroadcastSnap();
      }
    });
    window.addEventListener('beforeunload', flushOnHide);

    subscribeLiveChannel();
    subscribeDBChanges();
  }

  window.doLogin = async function() {
    showErr('auth-error-login', '');
    if (DEMO) { if(typeof window._origDoLogin==='function') window._origDoLogin(); return; }
    var email = (($('login-email')||{}).value||'').trim();
    var pass  = ($('login-password')||{}).value||'';
    if (!email||!pass) { showErr('auth-error-login','Please enter your email and password.'); return; }
    setBusy('login-btn', true, 'Sign In', 'Signing in…');
    try {
      var res = await sb.auth.signInWithPassword({email:email, password:pass});
      if (res.error) { showErr('auth-error-login', res.error.message); return; }
      hctSession = res.data.session;
      hctProfile = await fetchOrCreateProfile(res.data.user);
      await enterApp();
    } catch(e) {
      showErr('auth-error-login','Could not reach the server. Check your internet connection.');
    } finally { setBusy('login-btn', false, 'Sign In', 'Signing in…'); }
  };

  window.doSignup = async function() {
    showErr('auth-error-signup', '');
    if (DEMO) { if(typeof window._origDoLogin==='function') window._origDoLogin(); return; }
    var f    = (($('reg-fname')||{}).value||'').trim();
    var l    = (($('reg-lname')||{}).value||'').trim();
    var email= (($('reg-email')||{}).value||'').trim();
    var idno = (($('reg-idno')||{}).value||'').trim();
    var role = (($('reg-role')||{}).value||'student');
    var p1   = ($('reg-password')||{}).value||'';
    var p2   = ($('reg-confirm')||{}).value||'';
    if (!f||!l)     { showErr('auth-error-signup','Please enter your first and last name.'); return; }
    if (!email)     { showErr('auth-error-signup','Please enter your email address.'); return; }
    if (p1.length<6){ showErr('auth-error-signup','Password must be at least 6 characters.'); return; }
    if (p1!==p2)    { showErr('auth-error-signup','Passwords do not match.'); return; }
    setBusy('signup-btn', true, 'Create Account', 'Creating…');
    try {
      var res = await sb.auth.signUp({
        email:email, password:p1,
        options:{data:{full_name:f+' '+l, role:role, student_no:idno||null}}
      });
      if (res.error) { showErr('auth-error-signup', res.error.message); return; }
      if (res.data.session) {
        hctSession = res.data.session;
        hctProfile = await fetchOrCreateProfile(res.data.user);
        await enterApp();
      } else {
        if(typeof showScreen==='function') showScreen('s-login');
        showInfo('auth-info-login','Account created! Check your email to confirm, then sign in.');
      }
    } catch(e) {
      showErr('auth-error-signup','Could not reach the server. Check your internet connection.');
    } finally { setBusy('signup-btn', false, 'Create Account', 'Creating…'); }
  };

  window.hctLogout = async function() {
    try {
      if (!DEMO && hctSession) {
        if (broadcastTimer) clearInterval(broadcastTimer);
        broadcastChanges();
        await saveSharedState();
        await saveUserPrefs();
        await syncProgress(true);
        if (liveChannel) sb.removeChannel(liveChannel);
        if (dbChannel)   sb.removeChannel(dbChannel);
        await sb.auth.signOut();
      }
    } catch(e){}
    location.reload();
  };

  async function restoreSession() {
    if (DEMO) {
      var df=$('login-demo-fields'); if(df) df.style.display='';
      var dn=$('login-demo-note');   if(dn) dn.style.display='';
      var pw=$('login-password');    if(pw) pw.closest && pw.closest('.fg') && (pw.closest('.fg').style.display='none');
      return;
    }
    try {
      var res = await sb.auth.getSession();
      if (res.data && res.data.session) {
        hctSession = res.data.session;
        hctProfile = await fetchOrCreateProfile(res.data.session.user);
        await enterApp();
      }
    } catch(e) { console.warn('[HCT] session restore failed:', e); }
  }

  /* ─────────────────────────────────────────────────────────────────────
     10. ADMIN — Discharged Patients Panel
     ───────────────────────────────────────────────────────────────────── */

  window.showDischargedPanel = async function() {
    var isAdmin = hctProfile && hctProfile.role === 'admin';
    if (!isAdmin) { alert('Only administrators can view discharged patients.'); return; }

    var existing = $('discharged-modal');
    if (existing) existing.remove();

    var div = document.createElement('div');
    div.id  = 'discharged-modal';
    div.innerHTML =
      '<div class="fdash-overlay"><div class="fdash-modal" style="max-width:760px">' +
      '<div class="fdash-hdr">' +
        '<div class="fdash-title">Discharged Patients</div>' +
        '<button class="fdash-close" onclick="document.getElementById(\'discharged-modal\').remove()">✕</button>' +
      '</div>' +
      '<div class="fdash-body" id="discharged-body"><div class="fdash-loading">Loading…</div></div>' +
      '</div></div>';
    document.body.appendChild(div);

    var rows = await window.hctFetchDischargedPatients();
    var body = $('discharged-body');
    if (!body) return;

    if (!rows.length) {
      body.innerHTML = '<div class="fdash-empty">No discharged patients to display.</div>';
      return;
    }

    var html = '<div class="fdash-card"><div class="fdash-card-hdr">Discharged Patients (' + rows.length + ')</div>' +
      '<div class="fdash-table-scroll"><table class="fdash-table">' +
      '<thead><tr><th>Name</th><th>MRN</th><th>Ward</th><th>Diagnosis</th><th>Discharged</th><th>Action</th></tr></thead><tbody>';

    rows.forEach(function(row) {
      var dischargeStr = row.discharge_date
        ? new Date(row.discharge_date).toLocaleString('en-PH',{month:'short',day:'numeric',year:'numeric',hour:'2-digit',minute:'2-digit'})
        : '—';
      var safeId = String(row.id).replace(/[^a-zA-Z0-9_-]/g,'');
      html += '<tr>' +
        '<td><strong>' + esc(row.name) + '</strong></td>' +
        '<td class="fdash-muted">' + esc(row.mrn||'—') + '</td>' +
        '<td>' + esc(row.ward||'—') + '</td>' +
        '<td>' + esc((row.dx||'').substring(0,40) + ((row.dx||'').length>40?'…':'')) + '</td>' +
        '<td class="fdash-muted">' + esc(dischargeStr) + '</td>' +
        '<td><button class="fdash-grade-btn" style="background:#16A34A;font-size:11px;padding:5px 12px" ' +
          'onclick="hctRestorePatient(\'' + safeId + '\').then(function(){document.getElementById(\'discharged-modal\').remove();})">Restore</button>' +
        '</td></tr>';
    });

    html += '</tbody></table></div></div>';
    body.innerHTML = html;
  };

  /* ─────────────────────────────────────────────────────────────────────
     11. SYNC STATUS BADGE
     ───────────────────────────────────────────────────────────────────── */
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
      ? '<span style="width:7px;height:7px;border-radius:50%;background:#2BBFAD;display:inline-block"></span>Live sync on'
      : '<span style="width:7px;height:7px;border-radius:50%;background:#F59E0B;display:inline-block"></span>Saving…';
  }

  /* ─────────────────────────────────────────────────────────────────────
     12. FACULTY DASHBOARD
     ───────────────────────────────────────────────────────────────────── */
  var SECTION_LABELS = {
    'visit-summary':'Visit Summary','vitals':'Vital Signs','mar':'MAR',
    'notes':'Nursing Notes','orders':'Orders','labs':'Labs','io':'I&O',
    'screenings':'Screenings','resp-assess':'Resp Assessment',
    'physical-exam':'Physical Exam','admission-info':'Admission Info',
    'hpi':'HPI','careplan':'Care Plan','sbar':'SBAR','imaging':'Imaging',
    'immunizations':'Immunizations','submit-chart':'Submit & Debrief'
  };
  function secLabel(s) { return SECTION_LABELS[s]||s; }
  function esc(s) { return (typeof escHtml==='function') ? escHtml(String(s==null?'':s)) : String(s==null?'':s); }
  function fmtMin(ms) { return (Math.round((ms||0)/6000)/10)+' min'; }
  function fmtDateShort(iso) {
    if(!iso) return '—';
    try { return new Date(iso).toLocaleString('en-PH',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}); }
    catch(e) { return iso; }
  }

  var _dashData = null;

  async function fetchDashData() {
    if (DEMO||!sb) {
      var progress=[];
      var FP=window.FACULTY_PROGRESS||{};
      Object.keys(FP).forEach(function(pxId){
        Object.keys(FP[pxId]).forEach(function(sId){
          var prog=FP[pxId][sId];
          Object.keys(prog).forEach(function(sec){
            progress.push({student_no:sId,student_name:'Student Nurse',px_id:pxId,px_name:findPxName(pxId),section:sec,time_ms:prog[sec].time_ms,visits:prog[sec].visits,last_activity:null});
          });
        });
      });
      var subs=[];
      var SC=window.SUBMITTED_CHARTS||{};
      Object.keys(SC).forEach(function(pxId){
        (SC[pxId]||[]).forEach(function(s,i){
          subs.push({id:'demo-'+pxId+'-'+i,student_no:s.studentId,student_name:s.studentName,px_id:s.pxId,px_name:s.pxName,answers:s.answers,chart_snapshot:s.chartSnapshot,completion:s.completion||0,submitted_at:s.ts_iso,grade:s.grade!=null?s.grade:null,feedback:s.feedback||'',_demo:s});
        });
      });
      var studentsMap={};
      progress.concat(subs).forEach(function(r){if(r.student_no)studentsMap[r.student_no]={student_no:r.student_no,full_name:r.student_name,role:'student'};});
      return {students:Object.values(studentsMap),progress:progress,submissions:subs};
    }
    var pr = await Promise.all([
      sb.from('profiles').select('id,full_name,role,student_no,email,created_at').eq('role','student').order('full_name'),
      sb.from('student_progress').select('*').order('last_activity',{ascending:false}),
      sb.from('submissions').select('*').order('submitted_at',{ascending:false})
    ]);
    return {students:pr[0].data||[],progress:pr[1].data||[],submissions:pr[2].data||[]};
  }

  window.showFacultyDashboard = async function() {
    if (window.curUserRole!=='faculty') { alert('Faculty Dashboard is only accessible to faculty accounts.'); return; }
    var existing=$('faculty-dash-modal'); if(existing) existing.remove();
    var div=document.createElement('div');
    div.id='faculty-dash-modal';
    div.innerHTML=
      '<div class="fdash-overlay"><div class="fdash-modal">'+
      '<div class="fdash-hdr"><div class="fdash-title">Faculty Dashboard'+(DEMO?' <span class="fdash-demo-tag">DEMO — this browser only</span>':'')+
      '</div><div style="display:flex;gap:8px;align-items:center">'+
      '<button class="fdash-refresh" onclick="showFacultyDashboard()">↻ Refresh</button>'+
      '<button class="fdash-close" onclick="closeFacultyDash()">✕</button></div></div>'+
      '<div class="fdash-tabs">'+
      '<div class="fdash-tab active" data-tab="overview" onclick="fdashTab(\'overview\')">Overview</div>'+
      '<div class="fdash-tab" data-tab="students" onclick="fdashTab(\'students\')">Students</div>'+
      '<div class="fdash-tab" data-tab="subs" onclick="fdashTab(\'subs\')">Submissions & Grading</div>'+
      '<div class="fdash-tab" data-tab="analytics" onclick="fdashTab(\'analytics\')">Section Analytics</div>'+
      '</div>'+
      '<div class="fdash-body" id="fdash-body"><div class="fdash-loading">Loading live data…</div></div>'+
      '</div></div>';
    document.body.appendChild(div);
    try { _dashData=await fetchDashData(); fdashTab('overview'); }
    catch(e) { var b=$('fdash-body'); if(b) b.innerHTML='<div class="fdash-loading">Could not load data: '+esc(e.message||e)+'</div>'; }
  };

  window.closeFacultyDash = function() { var el=$('faculty-dash-modal'); if(el) el.remove(); };

  window.fdashTab = function(tab) {
    document.querySelectorAll('.fdash-tab').forEach(function(t){t.classList.toggle('active',t.getAttribute('data-tab')===tab);});
    var body=$('fdash-body'); if(!body||!_dashData) return;
    if(tab==='overview')   body.innerHTML=renderOverview();
    else if(tab==='students') body.innerHTML=renderStudents();
    else if(tab==='subs')  body.innerHTML=renderSubmissions();
    else if(tab==='analytics') body.innerHTML=renderAnalytics();
  };

  function statCard(val,lbl,color) {
    return '<div class="fdash-stat" style="border-top-color:'+(color||'var(--teal)')+'">'+
      '<div class="fdash-stat-val">'+val+'</div><div class="fdash-stat-lbl">'+lbl+'</div></div>';
  }
  function groupBy(rows,key){var out={};rows.forEach(function(r){var k=r[key]||'—';(out[k]=out[k]||[]).push(r);});return out;}

  function renderOverview() {
    var d=_dashData;
    var pending=d.submissions.filter(function(s){return s.grade==null;}).length;
    var graded=d.submissions.filter(function(s){return s.grade!=null;});
    var avgGrade=graded.length?Math.round(graded.reduce(function(a,s){return a+s.grade;},0)/graded.length):null;
    var totalTime=d.progress.reduce(function(a,r){return a+(r.time_ms||0);},0);
    var today=new Date(); today.setHours(0,0,0,0);
    var activeToday={};
    d.progress.forEach(function(r){if(r.last_activity&&new Date(r.last_activity)>=today)activeToday[r.student_no]=1;});
    var html='<div class="fdash-stat-grid">'+
      statCard(d.students.length||Object.keys(groupBy(d.progress,'student_no')).length,'Registered Students')+
      statCard(Object.keys(activeToday).length,'Active Today','#16A34A')+
      statCard(d.submissions.length,'Charts Submitted','#2563EB')+
      statCard(pending,'Pending Review',pending>0?'#D97706':'#16A34A')+
      statCard(avgGrade!=null?avgGrade+'%':'—','Avg. Grade','#7C3AED')+
      statCard(fmtMin(totalTime),'Total Charting Time','#0D9488')+
      '</div>';
    var recent=d.progress.slice(0,8);
    html+='<div class="fdash-card"><div class="fdash-card-hdr">Recent Charting Activity</div>';
    if(!recent.length) html+='<div class="fdash-empty">No student activity recorded yet.</div>';
    else {
      html+='<div class="fdash-table-scroll"><table class="fdash-table"><thead><tr><th>Student</th><th>Patient</th><th>Section</th><th>Time</th><th>Visits</th><th>Last Active</th></tr></thead><tbody>';
      recent.forEach(function(r){
        html+='<tr><td>'+esc(r.student_name)+' <span class="fdash-muted">('+esc(r.student_no)+')</span></td><td>'+esc(r.px_name)+'</td><td>'+esc(secLabel(r.section))+'</td><td>'+fmtMin(r.time_ms)+'</td><td>'+(r.visits||0)+'</td><td>'+fmtDateShort(r.last_activity)+'</td></tr>';
      });
      html+='</tbody></table></div>';
    }
    html+='</div>';
    var latest=d.submissions.slice(0,3);
    if(latest.length){
      html+='<div class="fdash-card"><div class="fdash-card-hdr">Latest Submissions</div>';
      latest.forEach(function(s){
        html+='<div class="fdash-sub-line"><div><strong>'+esc(s.student_name)+'</strong> — '+esc(s.px_name)+' <span class="fdash-muted">· '+fmtDateShort(s.submitted_at)+'</span></div>'+
          (s.grade!=null?'<span class="fdash-grade-pill graded">'+s.grade+'%</span>':'<span class="fdash-grade-pill pending">Pending</span>')+'</div>';
      });
      html+='<div style="padding:10px 14px"><a class="fdash-link" onclick="fdashTab(\'subs\')">Review & grade all submissions →</a></div></div>';
    }
    return html;
  }

  function renderStudents() {
    var d=_dashData;
    var byStudent=groupBy(d.progress,'student_no');
    var subsByStudent=groupBy(d.submissions,'student_no');
    var roster=d.students.length?d.students:Object.keys(byStudent).map(function(k){return{student_no:k,full_name:(byStudent[k][0]||{}).student_name||'Student'};});
    if(!roster.length) return '<div class="fdash-empty">No students registered yet.</div>';
    var html='<div class="fdash-card"><div class="fdash-card-hdr">Student Roster ('+roster.length+')</div><div class="fdash-table-scroll"><table class="fdash-table"><thead><tr><th>Student</th><th>ID No.</th><th>Patients Charted</th><th>Sections</th><th>Total Time</th><th>Submissions</th><th>Avg Grade</th><th>Last Active</th></tr></thead><tbody>';
    roster.forEach(function(st){
      var rows=byStudent[st.student_no]||[];
      var subs=subsByStudent[st.student_no]||[];
      var graded=subs.filter(function(s){return s.grade!=null;});
      var avg=graded.length?Math.round(graded.reduce(function(a,s){return a+s.grade;},0)/graded.length)+'%':'—';
      var pxSet={}; rows.forEach(function(r){pxSet[r.px_id]=1;});
      var totalMs=rows.reduce(function(a,r){return a+(r.time_ms||0);},0);
      var last=rows.map(function(r){return r.last_activity;}).filter(Boolean).sort().pop();
      html+='<tr><td><strong>'+esc(st.full_name)+'</strong></td><td class="fdash-muted">'+esc(st.student_no||'—')+'</td><td>'+Object.keys(pxSet).length+'</td><td>'+rows.length+'</td><td>'+fmtMin(totalMs)+'</td><td>'+subs.length+'</td><td>'+avg+'</td><td>'+fmtDateShort(last)+'</td></tr>';
      if(rows.length){
        var detail=rows.map(function(r){return esc(secLabel(r.section))+' ('+fmtMin(r.time_ms)+', '+r.visits+'×)';}).join(' · ');
        html+='<tr class="fdash-detail-row"><td colspan="8">'+esc(st.full_name).split(' ')[0]+'\'s sections: '+detail+'</td></tr>';
      }
    });
    html+='</tbody></table></div></div>';
    return html;
  }

  function renderSubmissions() {
    var d=_dashData;
    if(!d.submissions.length) return '<div class="fdash-empty">No charts submitted yet.</div>';
    var PQ=window.PEARLS_QUESTIONS||[];
    var html='';
    d.submissions.forEach(function(s){
      var snap=s.chart_snapshot||{};
      var ansHtml='';
      PQ.forEach(function(phase){
        var answered=phase.questions.filter(function(q){return s.answers&&s.answers[q.id];});
        if(!answered.length) return;
        ansHtml+='<div class="fdash-pearls-phase">'+esc(phase.icon||'')+' '+esc(phase.phase)+'</div>';
        answered.forEach(function(q){
          ansHtml+='<div class="fdash-pearls-qa"><div class="fdash-pearls-q">'+esc(q.text)+'</div><div class="fdash-pearls-a">'+esc(s.answers[q.id])+'</div></div>';
        });
      });
      var safeId=String(s.id).replace(/[^a-zA-Z0-9_-]/g,'');
      html+='<div class="fdash-card fdash-sub-card">'+
        '<div class="fdash-sub-hdr"><div><div class="fdash-sub-name">'+esc(s.student_name)+' <span class="fdash-muted">('+esc(s.student_no||'—')+')</span></div>'+
        '<div class="fdash-muted">Patient: '+esc(s.px_name)+' · Submitted: '+fmtDateShort(s.submitted_at)+' · Chart completion: '+(s.completion||0)+'%</div></div>'+
        (s.grade!=null?'<span class="fdash-grade-pill graded">Graded: '+s.grade+'%</span>':'<span class="fdash-grade-pill pending">Pending Review</span>')+'</div>'+
        '<div class="fdash-muted" style="margin:6px 0 10px">Orders: '+(snap.orders||0)+' · MAR meds: '+(snap.meds||0)+' · BP entries: '+(snap.bpEntries||0)+'</div>'+
        '<details class="fdash-details"><summary>PEARLS Reflection</summary>'+(ansHtml||'<div class="fdash-muted" style="padding:8px 0">No reflection answers recorded.</div>')+'</details>'+
        '<div class="fdash-grade-row">'+
        '<input type="number" min="0" max="100" id="fdash-grade-'+safeId+'" value="'+(s.grade!=null?s.grade:'')+'" placeholder="Grade %" class="fdash-grade-input"/>'+
        '<input type="text" id="fdash-fb-'+safeId+'" value="'+esc(s.feedback||'')+'" placeholder="Feedback for the student…" class="fdash-fb-input"/>'+
        '<button class="fdash-grade-btn" onclick="fdashSaveGrade(\''+esc(String(s.id))+'\',\''+safeId+'\')">Save Grade</button>'+
        '<span id="fdash-grade-msg-'+safeId+'" class="fdash-muted"></span>'+
        '</div></div>';
    });
    return html;
  }

  window.fdashSaveGrade = async function(subId, safeId) {
    var g=parseInt(($('fdash-grade-'+safeId)||{}).value,10);
    var fb=(($('fdash-fb-'+safeId)||{}).value||'').trim();
    var msg=$('fdash-grade-msg-'+safeId);
    if(isNaN(g)||g<0||g>100){if(msg)msg.textContent='Enter a grade from 0–100.';return;}
    if(msg)msg.textContent='Saving…';
    var sub=(_dashData.submissions||[]).filter(function(s){return String(s.id)===String(subId);})[0];
    if(DEMO){
      if(sub){sub.grade=g;sub.feedback=fb;if(sub._demo){sub._demo.grade=g;sub._demo.feedback=fb;}}
      if(msg)msg.textContent='Saved (demo).'; return;
    }
    try {
      var res=await sb.from('submissions').update({
        grade:g,feedback:fb,
        graded_by:hctProfile?hctProfile.full_name:'Faculty',
        graded_at:new Date().toISOString()
      }).eq('id',subId);
      if(res.error){if(msg)msg.textContent='Error: '+res.error.message;return;}
      if(sub){sub.grade=g;sub.feedback=fb;}
      if(msg)msg.textContent='Saved ✓';
    } catch(e){if(msg)msg.textContent='Save failed.';}
  };

  function renderAnalytics() {
    var d=_dashData;
    if(!d.progress.length) return '<div class="fdash-empty">No charting activity yet.</div>';
    var totals={};
    d.progress.forEach(function(r){
      var t=totals[r.section]=totals[r.section]||{time:0,visits:0,students:{}};
      t.time+=r.time_ms||0; t.visits+=r.visits||0; t.students[r.student_no]=1;
    });
    var secs=Object.keys(totals).sort(function(a,b){return totals[b].time-totals[a].time;});
    var maxTime=totals[secs[0]].time||1;
    var html='<div class="fdash-card"><div class="fdash-card-hdr">Engagement by Chart Section</div><div style="padding:14px">';
    secs.forEach(function(sec){
      var t=totals[sec];
      var pct=Math.max(2,Math.round(t.time/maxTime*100));
      html+='<div class="fdash-bar-row"><div class="fdash-bar-label">'+esc(secLabel(sec))+'</div>'+
        '<div class="fdash-bar-track"><div class="fdash-bar-fill" style="width:'+pct+'%"></div></div>'+
        '<div class="fdash-bar-val">'+fmtMin(t.time)+' · '+t.visits+' visits · '+Object.keys(t.students).length+' student(s)</div></div>';
    });
    html+='</div></div>';
    var low=secs.slice().reverse().slice(0,3);
    html+='<div class="fdash-card"><div class="fdash-card-hdr" style="background:#7A1F23">⚠ Areas Needing Improvement</div><div style="padding:14px">';
    low.forEach(function(sec){
      html+='<div class="fdash-sub-line"><span style="font-weight:600;color:#B91C1C">'+esc(secLabel(sec))+'</span><span class="fdash-muted">'+fmtMin(totals[sec].time)+' total · '+totals[sec].visits+' visits</span></div>';
    });
    html+='<div class="fdash-muted" style="margin-top:10px">These sections received the least attention from students.</div></div></div>';
    return html;
  }

  /* ─────────────────────────────────────────────────────────────────────
     13. MOBILE OFF-CANVAS SIDEBAR
     ───────────────────────────────────────────────────────────────────── */
  function setupMobileNav() {
    var btn = document.createElement('button');
    btn.id  = 'hct-mobile-nav-btn';
    btn.innerHTML = '☰ Chart Menu';
    btn.onclick = function() {
      var sbar = document.querySelector('.ehr-sidebar');
      var bd   = $('hct-mobile-nav-backdrop');
      if (sbar) sbar.classList.toggle('mobile-open');
      if (bd)   bd.classList.toggle('show', sbar && sbar.classList.contains('mobile-open'));
    };
    document.body.appendChild(btn);
    var backdrop = document.createElement('div');
    backdrop.id      = 'hct-mobile-nav-backdrop';
    backdrop.onclick = closeMobileNav;
    document.body.appendChild(backdrop);
    document.addEventListener('click', function(ev) {
      if (window.innerWidth > 768) return;
      var t = ev.target;
      if (t && (t.classList.contains('nav-child') ||
         (t.classList.contains('nav-item') && !t.classList.contains('parent')))) closeMobileNav();
    });
  }
  function closeMobileNav() {
    var sbar = document.querySelector('.ehr-sidebar');
    var bd   = $('hct-mobile-nav-backdrop');
    if (sbar) sbar.classList.remove('mobile-open');
    if (bd)   bd.classList.remove('show');
  }

  /* ─────────────────────────────────────────────────────────────────────
     14. INJECTED STYLES
     ───────────────────────────────────────────────────────────────────── */
  function injectStyles() {
    var css =
      '.fdash-overlay{position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:600;overflow-y:auto;padding:24px 12px}'+
      '.fdash-modal{background:var(--cream,#F8F7F3);max-width:1040px;margin:0 auto;border-radius:14px;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,.3)}'+
      '.fdash-hdr{background:var(--navy,#1B2A4A);padding:16px 20px;display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}'+
      '.fdash-title{color:#fff;font-family:"DM Serif Display",serif;font-size:20px}'+
      '.fdash-demo-tag{font-family:"DM Sans",sans-serif;font-size:10px;background:#D97706;color:#fff;padding:2px 8px;border-radius:9px;vertical-align:middle;margin-left:8px}'+
      '.fdash-close,.fdash-refresh{background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.25);color:#fff;border-radius:7px;padding:5px 12px;font-size:12px;cursor:pointer;font-family:"DM Sans",sans-serif}'+
      '.fdash-tabs{display:flex;background:#fff;border-bottom:1px solid var(--border,#E4E2DA);overflow-x:auto}'+
      '.fdash-tab{padding:11px 18px;font-size:13px;cursor:pointer;color:var(--text-muted,#6B7280);border-bottom:2.5px solid transparent;white-space:nowrap}'+
      '.fdash-tab.active{color:var(--teal-dark,#0D9488);border-bottom-color:var(--teal,#2BBFAD);font-weight:600}'+
      '.fdash-body{padding:18px}'+
      '.fdash-loading,.fdash-empty{text-align:center;padding:48px 20px;color:var(--text-muted,#6B7280);font-size:13px}'+
      '.fdash-stat-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px;margin-bottom:18px}'+
      '.fdash-stat{background:#fff;border:1px solid var(--border,#E4E2DA);border-radius:11px;padding:14px;border-top:3px solid var(--teal,#2BBFAD)}'+
      '.fdash-stat-val{font-size:26px;font-weight:700;color:var(--navy,#1B2A4A)}'+
      '.fdash-stat-lbl{font-size:10px;color:var(--text-muted,#6B7280);text-transform:uppercase;letter-spacing:.5px;font-weight:600;margin-top:2px}'+
      '.fdash-card{background:#fff;border:1px solid var(--border,#E4E2DA);border-radius:11px;margin-bottom:14px;overflow:hidden}'+
      '.fdash-card-hdr{background:var(--navy,#1B2A4A);color:#fff;padding:10px 14px;font-size:13px;font-weight:600}'+
      '.fdash-table-scroll{overflow-x:auto}'+
      '.fdash-table{width:100%;border-collapse:collapse;font-size:12px;min-width:640px}'+
      '.fdash-table th{padding:8px 12px;text-align:left;background:var(--cream,#F8F7F3);font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.4px;color:var(--text-muted,#6B7280);border-bottom:1px solid var(--border,#E4E2DA)}'+
      '.fdash-table td{padding:9px 12px;border-bottom:1px solid var(--border,#E4E2DA);vertical-align:middle;color:var(--text,#2A2E3A)}'+
      '.fdash-detail-row td{font-size:11px;color:var(--text-muted,#6B7280);background:var(--cream,#F8F7F3)}'+
      '.fdash-muted{color:var(--text-muted,#6B7280);font-size:11px;font-weight:400}'+
      '.fdash-sub-line{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:9px 14px;border-bottom:1px solid var(--border,#E4E2DA);font-size:12px;flex-wrap:wrap}'+
      '.fdash-grade-pill{font-size:10px;font-weight:700;padding:3px 9px;border-radius:10px;white-space:nowrap}'+
      '.fdash-grade-pill.graded{background:#DCFCE7;color:#16A34A}'+
      '.fdash-grade-pill.pending{background:#FEF3C7;color:#D97706}'+
      '.fdash-link{color:var(--teal-dark,#0D9488);font-size:12px;cursor:pointer;font-weight:600}'+
      '.fdash-sub-card{padding:14px}'+
      '.fdash-sub-hdr{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;flex-wrap:wrap}'+
      '.fdash-sub-name{font-size:13px;font-weight:700;color:var(--navy,#1B2A4A)}'+
      '.fdash-details{margin:8px 0;border:1px solid var(--border,#E4E2DA);border-radius:8px;padding:8px 12px;background:var(--cream,#F8F7F3)}'+
      '.fdash-details summary{font-size:12px;font-weight:600;color:var(--navy,#1B2A4A);cursor:pointer}'+
      '.fdash-pearls-phase{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.4px;color:var(--text-muted,#6B7280);margin:10px 0 4px}'+
      '.fdash-pearls-qa{padding:5px 0;border-bottom:1px solid var(--border,#E4E2DA)}'+
      '.fdash-pearls-q{font-size:11px;color:var(--text-muted,#6B7280)}'+
      '.fdash-pearls-a{font-size:12px;color:var(--navy,#1B2A4A);margin-top:2px;white-space:pre-wrap}'+
      '.fdash-grade-row{display:flex;gap:8px;align-items:center;margin-top:10px;flex-wrap:wrap}'+
      '.fdash-grade-input{width:90px;border:1.5px solid var(--border,#E4E2DA);border-radius:8px;padding:7px 10px;font-size:13px;font-family:"DM Sans",sans-serif}'+
      '.fdash-fb-input{flex:1;min-width:180px;border:1.5px solid var(--border,#E4E2DA);border-radius:8px;padding:7px 10px;font-size:13px;font-family:"DM Sans",sans-serif}'+
      '.fdash-grade-btn{background:var(--teal,#2BBFAD);border:none;color:#fff;border-radius:8px;padding:8px 16px;font-size:12px;font-weight:600;cursor:pointer;font-family:"DM Sans",sans-serif}'+
      '.fdash-bar-row{display:grid;grid-template-columns:150px 1fr auto;gap:10px;align-items:center;margin-bottom:8px}'+
      '.fdash-bar-label{font-size:12px;color:var(--navy,#1B2A4A);font-weight:500}'+
      '.fdash-bar-track{height:10px;background:var(--cream,#F8F7F3);border-radius:5px;border:1px solid var(--border,#E4E2DA);overflow:hidden}'+
      '.fdash-bar-fill{height:100%;background:linear-gradient(90deg,var(--teal,#2BBFAD),var(--navy,#1B2A4A));border-radius:5px}'+
      '.fdash-bar-val{font-size:11px;color:var(--text-muted,#6B7280);white-space:nowrap}'+
      '@media(max-width:640px){.fdash-bar-row{grid-template-columns:1fr;gap:4px}.fdash-body{padding:12px}}';
    var st = document.createElement('style');
    st.textContent = css;
    document.head.appendChild(st);
  }

  /* ─────────────────────────────────────────────────────────────────────
     BOOT
     ───────────────────────────────────────────────────────────────────── */
  function boot() {
    injectStyles();
    setupMobileNav();
    restoreSession();
    console.log('[HCT] Backend v12 — mode:', DEMO ? 'DEMO (localStorage)' : 'CLOUD + shared real-time tables');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
