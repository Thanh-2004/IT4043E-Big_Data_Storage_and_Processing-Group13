# MinIO

- Volumes: `pv` and `pvc` to declare persistent volume and claim of that volume for MinIO

- Deployment: num. of replicas (nodes), credentials, ports to access

- Services:

    + External: LoadBalancer to access API and console from outside K8s; use with `minikube tunnel`

    + Internal: ClusterIP, for internal connections including `mc` for buckets initialization and other apps to connect to

- Using URIs in K8s: use **service name**, not **deployment/pod name** (iiiiiiiiiiiiiiiiii)