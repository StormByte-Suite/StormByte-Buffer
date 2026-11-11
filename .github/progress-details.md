# Admission wait observer

## Research and execution scope (2026-11-11, before source edits)

- Requested bounded change: per-call, borrowed allocation-free observer for weighted
  Hopper/Sink PushFor waits only. Worktree initially clean on master at a3c2fbc.
- Admission decisions live in `lib/public/StormByte/buffer/hopper.txx`,
  `Hopper<T>::SharedState::Admit`. Local capacity is checked before TryReserve;
  the existing BudgetAccess::Wait call is outside the queue lock.
- Forward through Hopper's private Admit and Sink's SharedState::Admit without
  changing reservation, pressure, wait accounting, deadlines, or wake policy.
- Public types belong in byte_budget.hxx; both weighted PushFor overloads in
  hopper.hxx/sink.hxx receive a trailing defaulted observer.
- Tests belong in test/byte_budget_test.cxx and will synchronize via observer
  entry rather than timing/snapshot guesses, checking unlocked inspection,
  paired reasons, wake, cancellation, timeout, fast paths and Lease forwarding.
- Same release: update 4.0.0 Added in CHANGELOG.md; VERSION/tags untouched.
- Existing build: Release, clang++-22, ENABLE_TEST=ON. Build with
  `cmake --build build -j"$(nproc)"`; test with `ctest --test-dir build/test`.
- No task.md, assessment, scenario instructions, scenario skill root, or
  task-related skills were forwarded; no task folder was created.
  Instruction lookup found no matching C++ skill or Execution extension.
  Decomposition verdict: atomic, a single observer contract at one existing wait
  site with forwarding/tests/docs; no scenario Execution/Breakdown Hints supplied.

## Implementation and validation

- Modified byte_budget.hxx (exact agreed public types), hopper.hxx/hopper.txx
  and sink.hxx/sink.txx (defaulted trailing observer and forwarding), README.md,
  CHANGELOG.md (existing 4.0.0 entry only), test/byte_budget_test.cxx.
- The cause is selected under Hopper's admission lock. Only TryReserve
  WouldBlock selects SharedBudget; local denial remains LocalCapacity. Paired
  callbacks surround the existing BudgetAccess::Wait outside all Buffer locks.
  Reservation/admission policy, pressure/wait accounting, and optimizations
  are unchanged. No VERSION, tags, or CMake/testing placement changes.
- CODING_STYLE.md read in full and entire change set reviewed for its rules.
  Exact public Context/Changed names are required by the agreed API; the new
  test-helper members follow m_ naming. Observer is a standard-layout,
  trivially-copyable/destructible borrowed aggregate with no owned memory,
  STL owner, or cross-DLL destruction/assignment. Static assertions verify
  those properties. Callback is noexcept and runs on the PushFor caller.
- Tests cover Hopper and Sink weight/Lease forwarding, local element and byte
  denial, shared budget denial, success wakeups, cancellation, timeout,
  closure, zero-time/invalid/oversize/missing/closed fast paths, callback
  snapshot inspection, pairing/thread affinity, and LocalCapacity-to-SharedBudget
  transitions with pre-signalled wake events. Local tests exhaust both local
  and shared capacity to verify local precedence.
- All commands run in /home/stormbyte/Software/StormByte/StormByte/buffer,
  existing Release configuration, clang++-22, no environment overrides.
- `cmake --build build -j"$(nproc)"`: exit 0, whole configured build PASS,
  no errors or warnings emitted. Executed twice, including after final tests.
- `ctest --test-dir build/test --output-on-failure`: exit 0, 19/19 PASS
  (latest run 17.03 s); log build/test/Testing/Temporary/LastTest.log.
- `ctest --test-dir build/test -R '^ByteBudgetTests$' --repeat until-fail:20 --output-on-failure`:
  exit 0, all 20 repetitions PASS (4.43 s). CTest log uses the same path.
- `git diff --check`: exit 0. No validation blockers encountered.
- Consumer action: refresh Buffer headers (including .txx) from this commit
  in Multimedia's selected dependency prefix/cache, then rebuild Multimedia
  with `-j"$(nproc)"`. For an installed dependency use a user-owned prefix;
  no systemwide install was performed. Existing release archives remain
  unchanged until republished; no retag operation was performed here.

## Final review

- IntegrityReviewer verdict: PASS, no findings; record:
  `/home/stormbyte/Software/StormByte/StormByte/buffer/.github/admission-wait-review.md`.
  Full staged/unstaged/untracked scope was supplied. No blocking findings remain.
- Post-review advisory-only prose hygiene: separated README observer text into
  its own paragraph. No source or behavior changes after the passing checks.
- Protected VERSION, CMakeLists.txt, test/CMakeLists.txt and cmake directory
  verified unchanged. No modifications to Multimedia or other repositories,
  no systemwide install, and no tag operations.
