# Pip install commands

These commands can be used as an alternative to installing dependencies wvia requirements.txt. The differences between the two methods are:
* Installing with clean pip install will install latest versions of packages. This allows you to use the latest version of packages. Installing from requirements.txt will never automatically upgrade to new package versions.
* Installing from requirements.txt installs versions of dependencies that are known to work. This allows for guarantees that the installation will work. Installing from clean pip install might introduce dependency incompatibilities.

```bash
pip install pydantic pydantic-settings pendulum rich jupyterlab pandas "fastapi[all]"
```
