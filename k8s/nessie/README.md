# Nessie Catalog for Iceberg

UPDATE 3/1: Using Helm chart fails to connect to MongoDB for version storing. Now use simple `Deployment` with Docker image. See `kustomization.yaml` and mentioned resources.

```
helm install -n bigdata-pipeline nessie nessie-helm/nessie \
    --set replicaCount=1 \
    --set-string resources.requests.memory=500Mi \
    --set-string resources.limits.memory=1Gi \
    --set-string resources.requests.cpu=1 \
    -f ./k8s/nessie/values.yaml
```
Currently not resolved issue with connecting to MongoDB. Remove -f in final line of command to skip using MongoDB for version control; Nessie will store versioning metadata in-memory -> can use Nessie but will lose track of tables once K8s cluster is restarted.

Service for accessing Nessie catalog: `nessie:19120`

Full URI: `http://nessie:19120/api/v2`

To access web UI: `kubectl port-forward -n bigdata-pipeline svc/nessie 19120:19120` -> `localhost:19120`