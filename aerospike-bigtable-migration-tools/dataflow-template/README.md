# Dataflow Flex Template to import Aerospike backups into Cloud Bigtable

## Dependencies
This template utilizes `com.google.cloud.aerospike.backup-loader.BackupReader`, which requires shared libraries it
depends on to be present on the host machine (see https://github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/tree/aerospike_bigtable_migration_tools/aerospike-bigtable-migration-tools#dependencies
for details).
We're providing them with [custom worker image](https://docs.cloud.google.com/dataflow/docs/guides/build-container-image)
built [in a particular way](https://github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/tree/aerospike_bigtable_migration_tools/aerospike-bigtable-migration-tools#build-and-push-the-worker-image).

Note that the integration tests also require this image to be present (or the tests to be run with
`-DdirectRunnerTest=1` on machine containing all the shared library dependencies) - see `sdkContainerImage` property
in [pom.xml](pom.xml).
