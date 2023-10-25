# Vespa auth

Vespa expects credentials files to appear in the `~/.vespa/` directory in the following structure:

```
~/.vespa
├── YOUR_INSTANCE_URL
│   ├── data-plane-private-key.pem
│   └── data-plane-public-cert.pem
├── auth.json
└── config.yaml
```

the name of the directory `YOUR_INSTANCE_URL` should match the URL found in `config.yaml`, eg

```yaml
application: YOUR_INSTANCE_URL
target: cloud
```

You should be able to generate all of the above using the `vespa auth` command ([see vespa docs here](https://docs.vespa.ai/en/vespa-cli.html#login-and-init)).

The public key should be associated with your instance, with the private cert used to sign requests.

If you can't use the `~/.vespa` directory, you can specify the path to the credentials when creating a new `VespaSearchAdapter` object:

```python
adaptor = VespaSearchAdapter(
    instance_url="YOUR_INSTANCE_URL",
    cert_directory="PATH_TO_A_DIRECTORY_WITH_EQUIVALENT_STRUCTURE",
)
```
