## Contributing Guidelines

####Become the awesome favourite developer:
Before raising Pull Request to `dev` or `qa` branch
   1. Shield Your code against bugs - [How](https://github.com/moengage/MoEngage/wiki/shield)
   
   2. Did you remember to?
       - [ ] Add Test Case(s) [how to check](https://github.com/moengage/MoEngage/wiki/jenkins#unit-tests)
       - [ ] execute P0 Tests EndToEnd? [what is it](https://github.com/moengage/MoEngage/wiki/p0%20list)?
       - [ ] is [testable](https://github.com/moengage/MoEngage/wiki/QA_Acceptance_Checklist) ready for QC team with all required setup done?
       - [ ] Performance benchmarking?
       - [ ] Scalablity benchmarking?
       - [ ] is Service Available?
       - [ ] Secure?
       - [ ] Alerts?
       - [ ] Logs?
       - [ ] Monitoring?
       - [ ] Costs measurement?
       

####Debugging
* Don't use print statements.
* `from commons.helper.loggers import rawLogger, debugLogger, errorLogger`
* `debugLogger().debug(pythonObject, uniqueId = "lol")` uniqueId is not necessary
* `tail -100f /var/log/moengage/debug.log` log files

####Python Shell with our code
* `export MOE_DEPLOYMENT_ENV="prod"` set the deployment here
* `sudo -E python -m pyramid.scripts.pshell development.ini` python shell to test your functions

####Unit Tests:
* `tests/` folder
* Name test modules/files starting with `test_`
* Name test functions starting with `test_`
* Name test classes starting with `Test`
* Name test methods starting with `test_`
* Make sure all packages with test code have an `__init__.py`
* Follow similar directory structure to the actual code you are writing test cases for. (eg. `delight/viewhandlers/userhandler.py` -> `tests/test_delight/test_viewhandler/test_userhandler.py`)
* Set `Constants.env` as `unitTest` and form connections to dev db, you can get dev db constants from `Constants.DEV_DATABASE_NAME, host=Constants.DEV_DATABASE_HOST, port=Constants.DEV_DATABASE_PORT`
* You can find sample test cases in `tests/test_delight/test_viewhandler/test_user_handler_initialize.py`.
* Run test cases using `nosetests ./tests/ --with-coverage --cover-html --cover-html-dir=./delight/static/testreports/coverage/ --cover-package=delight --cover-erase --with-id`
* Browse to `http://your-dev-machine-ip/static/testreports/coverage/index.html` to see your coverage report.
* You can run only the failed test cases by replacing `--with-id` flag with `--failed` falg

So the development flow will be.
* Write code, tests
* run `nosetests ./tests/ --with-coverage --cover-html --cover-html-dir=./delight/static/testreports/coverage/ --cover-package=delight --cover-erase --with-id`
* If test case failures, say 4 Errors, only run those test cases by replacing `--with-id` with `--failed`
* `nosetests ./tests/ --with-coverage --cover-html --cover-html-dir=./delight/static/testreports/coverage/ --cover-package=delight --cover-erase --failed`
* Keep fixing failed test cases and run test cases with `--failed` flag, it keeps ignoring the passed test cases once they pass.

####Style:
* Tab equals 4 spaces
    * if you are using vim then update your .vimrc settings as below
    ```
    :set smartindent shiftwidth=4 softtabstop=4 expandtab
    ```
    * if you are using sublime text editor update your sublime text editor settings in preference.
    ```
    { "tab_size": 4, "translate_tabs_to_spaces": true }
    ```
    * if you are using any other Text Editor, set the tab space to 4 and update your settings such that 4 spaces are inserted on pressing tab key
* No tabs and trailing spaces allowed
* Standard python style guidelines

####Every time you are developing a new feature these are the steps you need to follow:
* Update your local dev to the latest moengage/moengage remote(origin)
```
> git checkout dev
> git pull origin dev
```
* Create a new branch from your updated master, so that you start from the latest moengage code.
```
> git checkout -b feature/newfeature
```
* Work on your new features and commit them..
```
commits relating to the awesome new feature
test them rigorously
```
* Push the new branch that contains the feature on to your github fork, in my case to syllogismos/moengage
```
> git push origin feature/newfeature
```

* Now update your local dev with the remote. (by the time you are developing, remote master branch might have changed)
```
> git checkout dev
> git pull origin dev
```

* Rebase your branch with master (this will ensure that everbody else's changes are there in your local feature/newfeature branch)
```
> git checkout feature/newfeature
> git rebase dev (you must be in your branch while rebasing)
```

* If there are any conflicts, resolve the conflicts. let's say I have conflict in test.py
```
after resolving conflict I will type two below commands
> git add test.py
> git rebase --continue
if you again get a conflict, resolve it and repeat the above two commands. (keep doing this until rebase is complete)
```

* after rebase is complete, push your local branch to remote
```
> git push origin feature/newfeature 
(you will need to add -f in case you encountered with conflict while rebase)
```

* Then create a pull request from your branch `moengage/feature/newfeature`, with base as `moengage/moengage/dev`

* A sample pull request looks like [this](https://github.com/moengage/MoEngage/pull/412)

* In a pull request you can see if your branch is mergeable with the dev. (if you do rebase properly it would be mergable with dev always)

* You can also see what additional commits are you merging on to the master

* Detailed `diff` of the entire pull request

* And you could also discuss regarding the pull request and changes on there and get is review if possible

* There should be another person who will review the code for probable bugs and mishaps and the reviewer should do a +1 comment or if there are bugs or clarification required, can comment about the same.

* If everything is ok, the reviewer should merge into dev. Not the person who develops.

* On the release day, @satya will merge dev into master and make a release via melora.

#### Important patch on a particular machine.

* Fix the bug on latest `dev` and take the `patch` as shown.

```
> git diff --full-index > /tmp/PATCH_NAME.diff
add --binary in case your file has diff in image files or any files which are not text files.
```

* Copy that patch to `melora` server.
```
> scp -i ~/devOps.cer  ubuntu@melora.moengage.com: /tmp/PATCH_NAME.diff ~/patch/
```

* How to apply the patch
```
patch -p1 < /tmp/PATCH_NAME.diff 
We can also achieve same by git apply ( Usefull when we have diff in binary files like images/etc)
```
* And tell @satya the FabTag that the patch should go into and the patch file name.

* Just make sure the patch you submitted on a production machine is in `dev` branch as a commit. It is a must.

#### Release Process
[Read Here](https://github.com/moengage/MoEngage/wiki/Release-Cycle)
