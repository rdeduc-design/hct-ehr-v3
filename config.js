/* ════════════════════════════════════════════════════════════════════
   HCT EHR — Backend Configuration
   ════════════════════════════════════════════════════════════════════
   1. Create a free project at https://supabase.com
   2. Run supabase/schema.sql in the SQL Editor (one time)
   3. Paste your Project URL and anon public key below
      (Supabase Dashboard → Settings → API)

   The anon key is SAFE to publish — access is enforced by Row Level
   Security in the database. NEVER paste the service_role key here.

   Leave both values empty ("") to run in DEMO MODE:
   the app works fully, but data lives only in the browser session.
   ════════════════════════════════════════════════════════════════════ */
window.HCT_CONFIG = {
  SUPABASE_URL: "",        // e.g. "https://abcdefghijklm.supabase.co"
  SUPABASE_ANON_KEY: ""    // e.g. "eyJhbGciOiJIUzI1NiIsInR5cCI6..."
};
