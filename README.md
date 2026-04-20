# Temporal Proxy

The main repo for the universal Temporal proxy. This is very much a WIP.

## Deployment

We provide a Helm chart for deploying the Temporal proxy. This is the recommended deployment option, though you can of
course build, run, and deploy the binaries however you like.

### Helm Chart

You'll need [Helm](https://helm.sh/) installed, which you can get from [here](https://helm.sh/docs/intro/install). Once
installed, you'll need to add our repo as follows:

```bash
helm repo add temporal-proxy https://github.io/temporalio/temporal-proxy
```

Once you've got the repo, you can install the chart with the following:

```bash
helm install <release-name> temporal-proxy/temporal-proxy [OPTIONS]

For more details, see the chart's [README](/charts/temporal-proxy/README.md).
```
