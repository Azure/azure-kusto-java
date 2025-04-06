- Update `revision` field in the pom files to the new version:
  - `pom.xml` in the root directory
  - `ingest/pom.xml` 
  - `data/pom.xml`
  - `quickstart/pom.xml`
  - `sample/pom.xml`

- Update `CHANGELOG.md` with the new version and date. Make sure it complies with the [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format.

- Tag the new version in the repository:
  ```bash
  git tag -a vX.Y.Z -m "Release version X.Y.Z"
  ```
  
- Push the tag to the remote repository:
  ```bash
    git push --tags
    ```
  
- The GitHub Actions workflow will automatically create a release on GitHub with the new version and the contents of `CHANGELOG.md`.

- For now, some of the automations may not work as expected. You might need to upload the files to the storage yourselves , or trigger the release via https://dev.azure.com/azure-sdk/internal/_build?definitionId=1809.
