# Release Process

* build + upload to bintray 
* verify files in bintray
* publish files in bintray
* sync bintray to mvn central



# Build

```bash
./gradlew clean build bintrayUpload -Pversion=0.0.5 -PbintrayUser=<bintray-user> -PbintrayApiKey=<bintray-apikey>
```

# Verify new version in bintray

* Open a browser to https://bintray.com/adobe-apiplatform/maven/mesos-actor
* Check for the new version, and examine files available (jar should be named `mesos-actor-<version>.jar`)

# Publish files in bintray

* Open a browser to https://bintray.com/adobe-apiplatform/maven/mesos-actor
* There should be a notice like `Notice: You have 8 unpublished item(s) for this version (expiring in 6 days and 22 hours) Discard | Publish`
* Click the `Publish` link

# Sync bintray to maven central

* Open a browser to https://bintray.com/adobe-apiplatform/maven/mesos-actor
* Click the `Maven Central` tab
* Enter `adobe_api_platform` for `User token key`
* Enter the password for `User token password`
* Wait for sync to complete
* Verify `Last Sync Status` on right side; should be: `Last Sync Status: Successfully synced and closed repo.`

Once sync completes, artifacts will be available in maven central. 

_Note that the maven central index takes some time to update, so browsing the index the artifacts may not be immediately visible._

 
