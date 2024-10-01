# Data Collector Connector Content Stream Rawdata

![Build Status](https://img.shields.io/github/actions/workflow/status/descoped/data-collector-connector-content-stream-rawdata/coverage-and-sonar-analysis.yml)
![Latest Tag](https://img.shields.io/github/v/tag/descoped/data-collector-connector-content-stream-rawdata)
![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=descoped_data-collector-connector-content-stream-rawdata&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=descoped_data-collector-connector-content-stream-rawdata) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=descoped_data-collector-connector-content-stream-rawdata&metric=coverage)](https://sonarcloud.io/summary/new_code?id=descoped_data-collector-connector-content-stream-rawdata)
[![Snyk Security Score](https://snyk.io/test/github/descoped/data-collector-connector-content-stream-rawdata/badge.svg)](https://snyk.io/test/github/descoped/data-collector-connector-content-stream-rawdata)

## Content Encryption

To enable encryption, configure the following properties in `application-*.properties:

```
rawdata.encryption.key=PASSPHRASE
rawdata.encryption.salt=SALT
```

This enables AES-128 or AES-256 GCM encryption of rawdata position entries. The consumer must add the
`io.descoped.encryption:entryption-util:VERSION` and implement the `EncryptionClient`.

For more information about Data Collector, please refer to
the [Data Collector documentation](https://github.com/descoped/data-collector-project).
