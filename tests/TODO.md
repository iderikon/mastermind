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
