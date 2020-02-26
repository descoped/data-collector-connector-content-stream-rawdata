# Data Collector Connector Content Stream Rawdata

## Content Encryption

To enable encryption, configure the following properties in `application-*.properties:

```
rawdata.encryption.key=PASSPHRASE
rawdata.encryption.salt=SALT
```

This enables AES-128 GCM encryption of rawdata position entries. The consumer must add the `no.ssb.rawdata:rawdata-crypto-util:VERSION` and implement the `EncryptionClient`.


[![Build Status](https://drone.prod-bip-ci.ssb.no/api/badges/statisticsnorway/data-collector-connector-content-stream-rawdata/status.svg)](https://drone.prod-bip-ci.ssb.no/statisticsnorway/data-collector-connector-content-stream-rawdata)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/19c7b49d1711437c9d1061fddf9a2220)](https://www.codacy.com/manual/oranheim/data-collector-connector-content-stream-rawdata?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=statisticsnorway/data-collector-connector-content-stream-rawdata&amp;utm_campaign=Badge_Grade)
[![codecov](https://codecov.io/gh/statisticsnorway/data-collector-connector-content-stream-rawdata/branch/master/graph/badge.svg)](https://codecov.io/gh/statisticsnorway/data-collector-connector-content-stream-rawdata)

For more information about Data Collector, please refer to the [Data Collector documentation](https://github.com/statisticsnorway/data-collector-project).
