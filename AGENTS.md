# AGENTS – Operating Contract for Codex

> **Scope**  All autonomous runs executed by OpenAI Codex (or any runner that complies with the
> `AGENTS.md` convention) inside this repository MUST respect every rule below from start
> to finish – no exceptions.

---

## 1  Core Operating Principles
1. **Reflect ⇢ Act ⇢ Verify loop**  
   • Think *step‑by‑step* before each action.  
   • After acting, *re‑read* the diff/artefact and verify it satisfies the plan.  
   • Iterate until the task is 100 % solved – never exit half‑done.

2. **Finish‑line ownership**  
   You ― Codex ― are responsible for the *entire* deliverable.  
   Hand‑offs to the user are allowed *only* when:
   - Every checklist item in §2 passes, **and**  
   - All edge‑cases documented in the task prompt are handled.

3. **No residual bloat**  
   • Temporary tables, files, or other resources must be dropped/deleted in the same run.  
   • Commits must stay idempotent – repeated runs yield the same repo & data state.

4. **Safety & Governance**  
   • Follow the repo’s coding standards, linters, and licence headers.

---

## 2  Quality Checklist (blocker = ❌, pass = ✅)

| # | Item |
|---|------|
| 1 | All new/changed code is **type‑correct** & passes the linter |
| 2 | All DB ops are **atomic** (`TMP_*` swap, `DROP` temp) |
| 3 | Retries/back‑off added to every external call |
| 4 | Logging is **informative** yet secrets‑safe |
| 5 | Tests or notebooks demonstrate success for edge‑cases |
| 6 | README / docs updated if public API changes |

Codex must refuse to finish until every line is ✅.

---

## 4  Commit Etiquette & Security

* 1 commit per logical task, message in conventional‑commit style  
  `feat(etl): atomic temp swap & checkpoint update`  
* PR must link back to the originating task / ticket.  
* CI must pass (lint, tests) before merge.  
* Never expose keys, credentials, or customer data in code, logs, or docs.

---

**Remember:** If any rule conflicts with a future task prompt, *fail fast* and ask
for clarification rather than breaking this contract.
