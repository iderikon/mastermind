### DESCRIPTION

This file contains a list of test cases and notes on features which should
be covered with automatic tests.

The list is broken down by sections each of which groups tests related to some
functional unit or feature. The format of list is as follows:

#### %Name of a feature or a functional unit%:

* **%Test title%**. %Short description: what this test checks%.
  1. *%Title of test case #1%*. %Brief description of setup, action, and expected
  result%;
  2. *%Title of test case #2%*. `...`

### TESTS

#### Backend:

* **Stat commit errors**. This test checks if backend's `stat_commit_errors` is
  taken into account during `Backend` status recalculation (when errors occur,
  backend must be turned read-only).
  1. *First error*. This test case checks basic functionality. Backend is first
  initialized and enabled, then the status is calculated. It must be `OK`.
  The next `BackendStat` is coming with non-zero `stat_commit_errors`. After the
  status recalculation it must become `RO`.
  2. *Subsequent errors*. This case checks if backend remains in state `RO`
  both when nothing changes and when new errors occur. Send two updates:
  stat with the same number of commit errors and update with greater number
  of errors.
  3. *Restart*. This case checks if `Backend` turns into state `OK` after
  a clean restart. Read-only backend is updated with a stat with more recent
  `last_start` timestamp.
  4. *Reset*. Check if backend becomes `OK` when a new number of commit errors
  is less than previously accounted.
  5. *Read-only*. Repeat tests #3 and #4 with `status/read_only` set to 1. Make
  sure backend remains `RO`.
* **Read-only flag**. This test checks if value of `status/read_only` is taken
  into account. Backend must be turned into state `RO` when the flag is set.
  1. *Flag set*. Check if state transition from `OK` to `RO` works fine. Update
  a backend with `BackendStat` with `read_only` flag set to 1.
  2. *Flag unset*. Check if reverse transition works. Update backend with stat
  with the flag unset.
  3. *Have ROFS errors*. Repeat previous two tests with non-zero
  `stat_commit_errors`. Status must not change and remain `RO`.
* **Group change**. This test verifies that change of a group served by backend
  is correctly handled.
  1. *Backend move*. Basic functionality check: update a backend with a stat
  with `group` changed to another existing. Check if backend and affected
  groups were updated: the backend and the new group are connected, the backend
  is unbound from the old group.
  2. *New group*. Update backend with a stat with group which didn't exist.
  Make sure the new group was created.
* **Stalled backends**. This test verifies status change of backends whose
  description wasn't received for configurable amount of time (default is 120
  seconds). Tests are conducted with timeout set to 1 second.
  1. *Stalled*. Basic functionality test. Update a backend, wait for two
  seconds, recalculate status and make sure it became `STALLED`.
  2. *Resurrection*. Check if resurrection turns backend into state `OK`.
  Update backend from previous test case with fresh stat.
  3. *Disabled backend*. Having backend in state `OK`, change `status/state`
  to `0` (`DNET_BACKEND_DISABLED`). State must become `STALLED`.

#### Couple:

* **DC sharing check**. This test verifies a stage of couple's status
calculation on which pairs of groups are checked for not having node backends
stored in the same DC. All test cases must pass for couples of two and of three
groups.
  1. *Basic setup #1*. This test does straightforward check of a simple setup.
  Given a couple with groups having one backend each, all backends are in
  different DCs, we should get couple's status `OK` as soon as
  `Couple::update_status` completes.
  2. *Basic setup #2*. This test does straightforward check of detection of
  wrong setup. There are two groups in a couple that have backends in the same
  DC. Couple's status should be reported `BROKEN`.
  3. *DHT groups #1*. This test makes sure that several backends in the same DC
  serving a DHT group are not considered as causing misconfig. So the setup
  is the same as in #1 except that one of the groups has several backends stored
  in the same DC.
  4. *DHT groups #2*. The same as previous one, but group backends are spread
  over different DCs.
  5. *DHT groups #3*. This test makes sure that detection of wrong setup still
  works fine for DHT groups. Conflicting backends are checked in all
  combinations of sequence numbers within their groups.
