# Developing sesam-py

1. Checkout the repository if you haven't
2. Create a new branch
3. Add your modifications
4. Make sure you increment the version number in the `sesam.py` and `install-latest.sh` files
4. Push branch to remote and create a PR in GitHub
5. When the PR passes tests, submit for review
6. After the PR is approved, merge it
7. Create a Release in GitHub, give it a tag which corresponds to the new version
8. Write down description in the Release 
9. A workflow should have been triggered when you created a Release which builds new assets and pushes them to the Release

## Installing pre-commit:
Commands:

```
$ pip install -r dev-requirements.txt
$ ./.pre-commit/install.sh
$ pre-commit install -f --hook-type pre-commit
```

And voila, you're good to go. Your python files will be formatted and checked when you commit your changes.
If there is an issue with your file you will be notified about it and can't push that file until it's fixed.


### [Back to main page](./README.md)