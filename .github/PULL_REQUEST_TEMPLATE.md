- [ ] CLA signed
- [ ] Title follows the naming scheme
- [ ] Backports considered
- [ ] Useful description provided

<!--
    Thanks for making the effort to create a Pull Request!
    Please read the comment in its entirety fist.

    If you haven't already, please sign our Contributor License Agreement at
    https://neo4j.com/developer/cla/
    Commits from accounts that have not signed the CLA will fail the CI and
    will not be reviewed. If you are a first-time contributor and have just
    signed the CLA, please include a note about this in the PR comment as some
    manual steps from our side are required.
-->

<!--
    Title:

    PRs targeting the current default branch (nightly driver) don't follow a
    fixed naming scheme.

    If your PR is a backport, please use the following naming scheme:

    [targeted version] description

    e.g.,

    [4.4] Remove a banana from the code
-->

<!--
    BACK PORTING STRATEGY

    When adding a fix, bumping dependencies, or similar, don't forget to
    backport the PR to all currently supported branches. At the moment these
    are:

     - 4.4 (mostly for the Java driver)
     - 5.0
     - 6.x

    New tests rarely need to be backported, as development of new features (in
    turn requiring new tests) happens primarily on the actively developed
    branch.
-->

<!--
    Description:

    Please include a summary of the changes in this PR and their purpose.
    Link any related issues or PRs.
-->
