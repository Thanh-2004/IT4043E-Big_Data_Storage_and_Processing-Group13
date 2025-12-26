# Run Kubernetes cluster

## 1. Create a cluster using Minikube
- Prereq: **Minikube** and **kubectl** installed
- Run: 
```
minikube start --memory <memory> --cpus <num-cpus>
```
Configure memory and number of CPUs designated to the cluster. By default (if not configured), 2GB memory and 1 core.

## 2. Commands
- Apply manifest files to run services: `kubectl apply -f <file-path>`
- View list of resources: `kubectl get <resource-type>`

    + List of namespaces: `kubectl get namespaces`

    + List of pods/nodes: change `namespaces` to `pods`/`nodes`

    + List of services in a namespace: `kubectl -n <namespace> get <kind>`

## 3. To run the system
- Ensure cluster is created and running (can use `kubectl get` commands above to check)

- Create namespace for system:
```
kubectl apply -f namespace.yaml
```

- Kafka:

    + Use Strimzi; install Helm chart:
        ```
        helm repo add strimzi https://strimzi.io/charts/
        ```
    
    + Install the chart to k8s cluster:
        ```
        helm install strimzi-release strimzi/strimzi-kafka-operator --namespace=bigdata-pipeline --create-namespace
        ```
        This installs the operator to the specified namespace. Here the namespace is set to the name used in `namespace.yaml`
    
    + Apply the Kafka Cluster resources:
        ```
        kubectl apply -f kafka/kafka-cluster.yaml
        ```
        This creates the NodePool that manages actual Kafka pods that are created, configured by the Kafka resource.

        Can create multiple NodePools, possibly one with 1 replica for dual-role (broker + KRaft controller) and one with 2 replicas as brokers only. For demo/testing, just use 1 dual-role to save resources
    
    + Create topic:
        ```
        kafka apply -f kafka/kafka-events-topic.yaml
        ```
        Creates the topic named `events` with configured number of partitions and replicas.
        
        Can modify name/configs in the yaml file, and can create more yaml files for more topics (or can write configs for new topics in the same file; see the cluster manifest file on how to separate multiple resources in the same file)
    
    