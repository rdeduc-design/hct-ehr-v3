# HCT EHR Simulation — v9 (Backend Edition)

**Healthcare and Technology Institute Inc. · "How Care Transforms"**

A full clinical-charting simulation platform for nursing students with real
login/signup, cloud-saved charts, shared student progress, chart submissions
with PEARLS reflections, and a faculty dashboard with grading.

---

## What's in this folder

```
hct-ehr/
├── index.html              ← the entire EHR app (open this)
├── js/
│   ├── config.js           ← paste your Supabase URL + anon key here
│   └── hct-backend.js      ← auth, cloud autosave, faculty dashboard, mobile nav
├── css/
│   └── responsive.css      ← tablet & mobile layout overrides
├── supabase/
│   └── schema.sql          ← database setup (run once in Supabase)
├── assets/                 ← HCT logo files
└── README.md               ← this guide
```

## Two modes

| Mode | When | What happens |
|---|---|---|
| **Demo mode** | `js/config.js` is left empty | App works fully, no password needed, data lives in the browser tab only |
| **Cloud mode** | Supabase URL + key filled in | Real accounts, charts auto-saved every 15 s, faculty sees **all** students live |

You can deploy in demo mode today and switch to cloud mode later just by
editing `js/config.js` — no other changes needed.

---

## Part 1 — Set up the backend (Supabase, free, ~10 minutes)

1. Go to **https://supabase.com** → *Start your project* → sign in with
   GitHub or Google → **New project**.
   - Name: `hct-ehr` · Database password: anything strong (save it) ·
     Region: **Southeast Asia (Singapore)**.
2. Wait ~2 minutes for the project to provision.
3. Left sidebar → **SQL Editor** → **New query** → open
   `supabase/schema.sql` from this folder, copy ALL of it, paste, press
   **Run**. You should see "Success. No rows returned."
4. Left sidebar → **Authentication → Sign In / Providers** → under *Email*,
   turn **OFF "Confirm email"**. (Recommended for classroom use so students
   can sign in immediately after registering. Leave it ON if you prefer
   email verification.)
5. Left sidebar → **Project Settings → API**. Copy:
   - **Project URL** (looks like `https://xxxxx.supabase.co`)
   - **anon public** key (long string starting with `eyJ…`)
6. Open `js/config.js` in any text editor and paste both values:

```js
window.HCT_CONFIG = {
  SUPABASE_URL: "https://xxxxx.supabase.co",
  SUPABASE_ANON_KEY: "eyJhbGciOi..."
};
```

> ✅ The **anon** key is safe to publish on GitHub — security is enforced by
> Row Level Security rules in the database.
> ❌ Never put the **service_role** key in this file.

### Create the faculty account

1. Open the deployed app → **Create one** → register with role
   **Clinical Instructor / Faculty**.
2. That's it — the signup form stores the role automatically.
3. (Optional) To promote an account later, run this in the SQL Editor:
   ```sql
   update public.profiles set role = 'faculty' where email = 'dean@hct.edu.ph';
   ```

---

## Part 2 — Deploy to GitHub Pages (free hosting)

1. Create a GitHub account (if needed) → **New repository** → name it
   `hct-ehr` → Public → Create.
2. On the repo page: **uploading an existing file** → drag the ENTIRE
   contents of this folder (`index.html`, `js/`, `css/`, `supabase/`,
   `assets/`) → **Commit changes**.
3. Repo → **Settings → Pages** → Source: **Deploy from a branch** →
   Branch: `main`, folder: `/ (root)` → **Save**.
4. Wait 1–2 minutes. Your app is live at:
   `https://<your-username>.github.io/hct-ehr/`

Using the command line instead:

```bash
cd hct-ehr
git init
git add .
git commit -m "HCT EHR v9 with backend"
git branch -M main
git remote add origin https://github.com/<your-username>/hct-ehr.git
git push -u origin main
# then enable Pages in repo Settings as above
```

**Any other host works too** (Netlify, Vercel, cPanel, a school server) —
this is a static site; just upload the folder.

---

## What the backend adds

- **Login / Signup** — real email + password accounts with roles
  (Student Nurse / Faculty / Admin), error messages, Enter-to-submit, and
  automatic session restore (refresh keeps you signed in).
- **Cloud autosave** — every chart entry (vitals, MAR, notes, labs, I&O,
  care plans, registered patients…) is snapshotted and saved every 15
  seconds and when you close the tab. Log in from any device and continue.
- **Sign Out** button in the top bar.
- **Shared progress** — students' time-on-task per chart section syncs to
  the database so faculty see the whole cohort, not just one browser.
- **Submissions & grading** — submitted charts + PEARLS reflections land in
  the faculty dashboard, where faculty assign a grade (0–100) and feedback.
- **Improved Faculty Dashboard** — four tabs:
  - *Overview*: registered students, active today, pending reviews, average
    grade, total charting time, recent activity feed
  - *Students*: full roster with per-student patients, sections, time,
    submissions, and average grade
  - *Submissions & Grading*: every PEARLS reflection with inline grading
  - *Section Analytics*: engagement bars per chart section + "areas needing
    improvement" teaching-gap callout
- **Fully responsive** — stacked auth screens, horizontally scrolling tabs,
  an off-canvas **☰ Chart Menu** drawer on phones, scrollable tables, and
  16 px inputs to stop iOS zoom.
- **Labs rendering fix** — lab names containing `<` / `>` (e.g.
  `DTaP (<7 yrs)`) no longer break the Labs section (escaped with
  `escHtml()`).

---

## Troubleshooting

| Problem | Fix |
|---|---|
| "Demo mode" note shows even after configuring | Check `js/config.js` — both values must be inside the quotes, no extra spaces; hard-refresh (Ctrl+Shift+R) |
| Signup says "Email not confirmed" | Supabase → Authentication → Sign In / Providers → turn off **Confirm email**, or click the link in the confirmation email |
| Faculty Dashboard says "only accessible to faculty" | The account's role is `student` — promote it with the SQL in Part 1 |
| Faculty sees no students | Students appear after they sign up **and** open a patient chart (progress syncs every 30 s) |
| "Could not reach the server" | Check internet connection and that the Project URL has no trailing slash |
| Changes not saving | Look for the small "Cloud sync on" dot in the top bar; check the browser console (F12) for messages starting with `[HCT]` |
| Page is blank after upload | Make sure `index.html` is at the repository **root**, not inside a subfolder |

---

## Security notes

- The `anon` key + Row Level Security = students can only read/write their
  own data; faculty can read everyone's progress and submissions and update
  grades. Chart states are private to each user.
- Aligned with RA 10173 (Data Privacy Act) practice: only name, email, role,
  and student number are stored; all simulation patients are fictional.

---

*HCT Academy R&D · Built for BSN pre-licensure simulation training and PNLE
readiness.*
