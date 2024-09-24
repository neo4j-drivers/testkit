Contributing
============

Setting up the development environment:
 * Install Python 3.8+
 * Install the requirements
   ```bash
   $ python3 -m pip install -U pip
   $ python3 -m pip install -Ur requirements.txt
   ```
   You have to repeat this step whenever a commit adds new dependencies.
 * Install the pre-commit hook, that will do some code-format-checking.
   ```bash
   $ pre-commit install
   ```
   Note that this is not an auto-formatter. It will alter some code, but
   mostly it will just complain about non-compliant code.  
   You can disable a certain check for a single line of code if you think
   your code-style if preferable. E.g.
   ```python
   assume_this_line(violates_rule_e123, and_w321)  # noqa: E123,W321
   ```
   Or use just `# noqa` to disable all checks for this line.  
   If you use `# noqa` on its own line, it will disable *all* checks for the
   whole file. Don't do that.  
   To disable certain rules for a whole file, check out
   `setup.cfg`.  
   If you want to run the checks manually, you can do so:
   ```bash
   $ pre-commit run --all-files
   # or
   $ pre-commit run --file path/to/a/file
   ```
