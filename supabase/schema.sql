-- ════════════════════════════════════════════════════════════════════
-- HCT EHR — Supabase Database Schema (run ONCE in the SQL Editor)
-- Supabase Dashboard → SQL Editor → New query → paste this file → Run
-- Safe to re-run: uses IF NOT EXISTS / OR REPLACE / DROP POLICY IF EXISTS
-- ════════════════════════════════════════════════════════════════════

-- ── 1. PROFILES ──────────────────────────────────────────────────────
-- One row per user; created automatically on signup by the trigger below.
create table if not exists public.profiles (
  id          uuid primary key references auth.users(id) on delete cascade,
  full_name   text,
  role        text not null default 'student' check (role in ('student','faculty','admin')),
  student_no  text,
  email       text,
  created_at  timestamptz not null default now()
);

-- Helper used by RLS policies. SECURITY DEFINER avoids recursive policy
-- evaluation when a profiles policy needs to read profiles.
create or replace function public.is_faculty()
returns boolean
language sql stable security definer set search_path = public as $$
  select exists (
    select 1 from public.profiles
    where id = auth.uid() and role in ('faculty','admin')
  );
$$;

-- Auto-create a profile whenever a new user signs up
create or replace function public.handle_new_user()
returns trigger
language plpgsql security definer set search_path = public as $$
begin
  insert into public.profiles (id, full_name, role, student_no, email)
  values (
    new.id,
    coalesce(new.raw_user_meta_data->>'full_name', split_part(new.email,'@',1)),
    coalesce(new.raw_user_meta_data->>'role', 'student'),
    new.raw_user_meta_data->>'student_no',
    new.email
  )
  on conflict (id) do nothing;
  return new;
end;
$$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
  after insert on auth.users
  for each row execute function public.handle_new_user();

-- ── 2. CHART STATES ──────────────────────────────────────────────────
-- One JSON document per user holding their entire EHR working state
-- (vitals, MAR, notes, labs, care plans, registered patients, etc.)
create table if not exists public.chart_states (
  user_id     uuid primary key references auth.users(id) on delete cascade,
  state       jsonb not null default '{}'::jsonb,
  updated_at  timestamptz not null default now()
);

-- ── 3. STUDENT PROGRESS ──────────────────────────────────────────────
-- Per-section time-on-task and visit counts, visible to faculty
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
create index if not exists idx_progress_activity on public.student_progress (last_activity desc);

-- ── 4. SUBMISSIONS ───────────────────────────────────────────────────
-- Submitted charts + PEARLS reflections; faculty grade them here
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
create index if not exists idx_submissions_time on public.submissions (submitted_at desc);

-- ── 5. ROW LEVEL SECURITY ────────────────────────────────────────────
alter table public.profiles         enable row level security;
alter table public.chart_states     enable row level security;
alter table public.student_progress enable row level security;
alter table public.submissions      enable row level security;

-- profiles: users manage their own row; faculty can read everyone
drop policy if exists "profiles_select_own"     on public.profiles;
drop policy if exists "profiles_select_faculty" on public.profiles;
drop policy if exists "profiles_insert_own"     on public.profiles;
drop policy if exists "profiles_update_own"     on public.profiles;
create policy "profiles_select_own"     on public.profiles for select using (auth.uid() = id);
create policy "profiles_select_faculty" on public.profiles for select using (public.is_faculty());
create policy "profiles_insert_own"     on public.profiles for insert with check (auth.uid() = id);
create policy "profiles_update_own"     on public.profiles for update using (auth.uid() = id);

-- chart_states: strictly private to each user
drop policy if exists "chart_states_all_own" on public.chart_states;
create policy "chart_states_all_own" on public.chart_states
  for all using (auth.uid() = user_id) with check (auth.uid() = user_id);

-- student_progress: students write their own; faculty read everything
drop policy if exists "progress_select_own"     on public.student_progress;
drop policy if exists "progress_select_faculty" on public.student_progress;
drop policy if exists "progress_insert_own"     on public.student_progress;
drop policy if exists "progress_update_own"     on public.student_progress;
create policy "progress_select_own"     on public.student_progress for select using (auth.uid() = user_id);
create policy "progress_select_faculty" on public.student_progress for select using (public.is_faculty());
create policy "progress_insert_own"     on public.student_progress for insert with check (auth.uid() = user_id);
create policy "progress_update_own"     on public.student_progress for update using (auth.uid() = user_id);

-- submissions: students create + view their own; faculty view all & grade
drop policy if exists "subs_select_own"      on public.submissions;
drop policy if exists "subs_select_faculty"  on public.submissions;
drop policy if exists "subs_insert_own"      on public.submissions;
drop policy if exists "subs_update_faculty"  on public.submissions;
create policy "subs_select_own"     on public.submissions for select using (auth.uid() = user_id);
create policy "subs_select_faculty" on public.submissions for select using (public.is_faculty());
create policy "subs_insert_own"     on public.submissions for insert with check (auth.uid() = user_id);
create policy "subs_update_faculty" on public.submissions for update using (public.is_faculty());

-- ════════════════════════════════════════════════════════════════════
-- ── 6. REAL-TIME SYNC ────────────────────────────────────────────────
-- Allow all authenticated users to read any chart_state (needed for cross-user
-- patient/data merge). Each user can still only write their own row.
drop policy if exists "chart_states_all_own" on public.chart_states;

create policy "chart_states_select_authenticated" on public.chart_states
  for select using (auth.uid() is not null);

create policy "chart_states_insert_own" on public.chart_states
  for insert with check (auth.uid() = user_id);

create policy "chart_states_update_own" on public.chart_states
  for update using (auth.uid() = user_id) with check (auth.uid() = user_id);

create policy "chart_states_delete_own" on public.chart_states
  for delete using (auth.uid() = user_id);

-- Enable Realtime on chart_states so Supabase broadcasts row changes
alter publication supabase_realtime add table public.chart_states;
