-- ════════════════════════════════════════════════════════════════════
-- HCT EHR — Supabase Database Schema (v2)
-- Safe to re-run: uses IF NOT EXISTS / OR REPLACE / DROP POLICY IF EXISTS
-- ════════════════════════════════════════════════════════════════════

-- ── 1. PROFILES ──────────────────────────────────────────────────────
create table if not exists public.profiles (
  id          uuid primary key references auth.users(id) on delete cascade,
  full_name   text,
  role        text not null default 'student' check (role in ('student','faculty','admin')),
  student_no  text,
  email       text,
  created_at  timestamptz not null default now()
);

create or replace function public.is_faculty()
returns boolean language sql stable security definer set search_path = public as $$
  select exists (select 1 from public.profiles where id = auth.uid() and role in ('faculty','admin'));
$$;

create or replace function public.is_admin()
returns boolean language sql stable security definer set search_path = public as $$
  select exists (select 1 from public.profiles where id = auth.uid() and role = 'admin');
$$;

create or replace function public.handle_new_user()
returns trigger language plpgsql security definer set search_path = public as $$
begin
  insert into public.profiles (id, full_name, role, student_no, email)
  values (
    new.id,
    coalesce(new.raw_user_meta_data->>'full_name', split_part(new.email,'@',1)),
    coalesce(new.raw_user_meta_data->>'role', 'student'),
    new.raw_user_meta_data->>'student_no',
    new.email
  ) on conflict (id) do nothing;
  return new;
end;
$$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
  after insert on auth.users
  for each row execute function public.handle_new_user();

-- ── 2. CHART STATES (kept for backward compat / per-user prefs) ──────
create table if not exists public.chart_states (
  user_id     uuid primary key references auth.users(id) on delete cascade,
  state       jsonb not null default '{}'::jsonb,
  updated_at  timestamptz not null default now()
);

-- ── 3. SHARED EHR STATE ───────────────────────────────────────────────
create table if not exists public.shared_ehr_state (
  state_key       text not null,
  px_id           text not null default '__global',
  data            jsonb not null default '{}',
  updated_by      uuid references auth.users(id),
  updated_by_name text,
  updated_at      timestamptz not null default now(),
  primary key (state_key, px_id)
);

-- ── 4. PATIENTS ───────────────────────────────────────────────────────
create table if not exists public.ehr_patients (
  id            text primary key,
  mrn           text,
  name          text not null,
  age           integer,
  sex           text,
  room          text,
  ward          text not null,
  section_type  text not null default 'inpatient',
  dx            text,
  status        text not null default 'admitted',
  physician     text,
  admitted      text,
  dob           text,
  allergies     jsonb not null default '[]',
  photo         text,
  extra         jsonb not null default '{}',
  is_discharged boolean not null default false,
  discharge_date timestamptz,
  admitted_at   timestamptz not null default now(),
  created_by    uuid references auth.users(id),
  updated_by    uuid references auth.users(id),
  updated_at    timestamptz not null default now(),
  deleted_at    timestamptz,
  deleted_by    uuid references auth.users(id)
);
create index if not exists idx_ehr_patients_ward on public.ehr_patients(ward, section_type);
create index if not exists idx_ehr_patients_discharged on public.ehr_patients(is_discharged, deleted_at);

-- ── 5. NOTIFICATIONS ──────────────────────────────────────────────────
create table if not exists public.ehr_notifications (
  id              uuid primary key default gen_random_uuid(),
  notif_type      text not null default 'info',
  store_key       text,
  section         text,
  px_id           text,
  px_name         text,
  message         text not null,
  created_by      uuid references auth.users(id),
  created_by_name text,
  created_by_role text default 'student',
  created_at      timestamptz not null default now(),
  read_by         jsonb not null default '[]'
);
create index if not exists idx_ehr_notifs_created on public.ehr_notifications(created_at desc);

-- ── 6. STUDENT PROGRESS ───────────────────────────────────────────────
create table if not exists public.student_progress (
  id            bigint generated always as identity primary key,
  user_id       uuid not null references auth.users(id) on delete cascade,
  student_no    text,
  student_name  text,
  px_id         text not null,
  px_name       text,
  section       text not null,
  time_ms       bigint not null default 0,
  visits        integer not null default 0,
  last_activity timestamptz not null default now(),
  unique (user_id, px_id, section)
);
create index if not exists idx_progress_activity on public.student_progress(last_activity desc);

-- ── 7. SUBMISSIONS ────────────────────────────────────────────────────
create table if not exists public.submissions (
  id             uuid primary key default gen_random_uuid(),
  user_id        uuid not null references auth.users(id) on delete cascade,
  student_no     text,
  student_name   text,
  px_id          text not null,
  px_name        text,
  answers        jsonb not null default '{}'::jsonb,
  chart_snapshot jsonb not null default '{}'::jsonb,
  completion     integer not null default 0,
  submitted_at   timestamptz not null default now(),
  grade          integer check (grade between 0 and 100),
  feedback       text,
  graded_by      text,
  graded_at      timestamptz
);
create index if not exists idx_submissions_time on public.submissions(submitted_at desc);

-- ── 8. ROW LEVEL SECURITY ─────────────────────────────────────────────
alter table public.profiles           enable row level security;
alter table public.chart_states       enable row level security;
alter table public.shared_ehr_state   enable row level security;
alter table public.ehr_patients       enable row level security;
alter table public.ehr_notifications  enable row level security;
alter table public.student_progress   enable row level security;
alter table public.submissions        enable row level security;

-- profiles
drop policy if exists "profiles_select_own"     on public.profiles;
drop policy if exists "profiles_select_faculty" on public.profiles;
drop policy if exists "profiles_insert_own"     on public.profiles;
drop policy if exists "profiles_update_own"     on public.profiles;
create policy "profiles_select_own"     on public.profiles for select using (auth.uid() = id);
create policy "profiles_select_faculty" on public.profiles for select using (public.is_faculty());
create policy "profiles_insert_own"     on public.profiles for insert with check (auth.uid() = id);
create policy "profiles_update_own"     on public.profiles for update using (auth.uid() = id);

-- chart_states
drop policy if exists "chart_states_all_own"                on public.chart_states;
drop policy if exists "chart_states_select_authenticated"   on public.chart_states;
drop policy if exists "chart_states_insert_own"             on public.chart_states;
drop policy if exists "chart_states_update_own"             on public.chart_states;
drop policy if exists "chart_states_delete_own"             on public.chart_states;
create policy "chart_states_select_authenticated" on public.chart_states
  for select using (auth.uid() is not null);
create policy "chart_states_insert_own" on public.chart_states
  for insert with check (auth.uid() = user_id);
create policy "chart_states_update_own" on public.chart_states
  for update using (auth.uid() = user_id) with check (auth.uid() = user_id);
create policy "chart_states_delete_own" on public.chart_states
  for delete using (auth.uid() = user_id);

-- shared_ehr_state: all authenticated users can read and write
drop policy if exists "shared_ehr_all" on public.shared_ehr_state;
create policy "shared_ehr_all" on public.shared_ehr_state
  for all using (auth.uid() is not null) with check (auth.uid() is not null);

-- ehr_patients
drop policy if exists "px_select_active"      on public.ehr_patients;
drop policy if exists "px_select_discharged"  on public.ehr_patients;
drop policy if exists "px_insert"             on public.ehr_patients;
drop policy if exists "px_update"             on public.ehr_patients;
create policy "px_select_active" on public.ehr_patients
  for select using (auth.uid() is not null and deleted_at is null);
create policy "px_select_discharged" on public.ehr_patients
  for select using (public.is_admin());
create policy "px_insert" on public.ehr_patients
  for insert with check (auth.uid() is not null);
create policy "px_update" on public.ehr_patients
  for update using (auth.uid() is not null) with check (auth.uid() is not null);

-- ehr_notifications
drop policy if exists "notif_select" on public.ehr_notifications;
drop policy if exists "notif_insert" on public.ehr_notifications;
drop policy if exists "notif_update" on public.ehr_notifications;
create policy "notif_select" on public.ehr_notifications
  for select using (auth.uid() is not null);
create policy "notif_insert" on public.ehr_notifications
  for insert with check (auth.uid() is not null);
create policy "notif_update" on public.ehr_notifications
  for update using (auth.uid() is not null);

-- student_progress
drop policy if exists "progress_select_own"     on public.student_progress;
drop policy if exists "progress_select_faculty" on public.student_progress;
drop policy if exists "progress_insert_own"     on public.student_progress;
drop policy if exists "progress_update_own"     on public.student_progress;
create policy "progress_select_own"     on public.student_progress for select using (auth.uid() = user_id);
create policy "progress_select_faculty" on public.student_progress for select using (public.is_faculty());
create policy "progress_insert_own"     on public.student_progress for insert with check (auth.uid() = user_id);
create policy "progress_update_own"     on public.student_progress for update using (auth.uid() = user_id);

-- submissions
drop policy if exists "subs_select_own"     on public.submissions;
drop policy if exists "subs_select_faculty" on public.submissions;
drop policy if exists "subs_insert_own"     on public.submissions;
drop policy if exists "subs_update_faculty" on public.submissions;
create policy "subs_select_own"     on public.submissions for select using (auth.uid() = user_id);
create policy "subs_select_faculty" on public.submissions for select using (public.is_faculty());
create policy "subs_insert_own"     on public.submissions for insert with check (auth.uid() = user_id);
create policy "subs_update_faculty" on public.submissions for update using (public.is_faculty());

-- ── 9. REALTIME PUBLICATIONS ──────────────────────────────────────────
alter publication supabase_realtime add table public.shared_ehr_state;
alter publication supabase_realtime add table public.ehr_patients;
alter publication supabase_realtime add table public.ehr_notifications;
