# Nessie Catalog for Iceberg

```
helm install -n bigdata-pipeline nessie nessie-helm/nessie \
    --set replicaCount=1 \
    --set-string resources.requests.memory=500Mi \
    --set-string resources.limits.memory=1Gi \
    --set-string resources.requests.cpu=1 \
    -f ./k8s/nessie/values.yaml
```

Service for accessing Nessie catalog: `nessie:19120`

Full URI: `http://nessie:19120/api/v2`