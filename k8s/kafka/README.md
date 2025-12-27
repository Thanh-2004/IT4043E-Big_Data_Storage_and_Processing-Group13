# Kafka Cluster on Kubernetes

Details on how to configure resources for the Kafka cluster. Shortcut: `kubectl apply -k <path-to-this-directory>`.

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
    
- To send messages to Kafka:

    + Context:
        - Kafka cluster deployed on K8s (assuming with Minikube);
        - A `producer.py` program that sends messages to Kafka brokers using Kafka Producer client
    
    + First, expose an external service to Kafka. In `kafka-cluster.yaml`, declare a `loadbalancer` or `nodeport` service under `spec.kafka.listeners` (already present)

    + Since Minikube does not expose an external IP, usual configs/settings to use NodePort or LoadBalancer would fail. Use Minikube commands to use the above external service:
        - If use NodePort:
            ```
            minikube -n bigdata-pipeline service kafka-cluster-kafka-external-bootstrap --url
            ```
            This prints out an address that can be used to access the cluster. Usually `localhost:<port>`.
            
            (Note that above command assumes the namespace and service name as used in previous parts; make changes as necessary)
            
            Idea: the port printed is the local machine's port that is mapped to the Minikube node's port used for the NodePort service.
        - If use LoadBalancer:
            ```
            minikube tunnel
            ```
            This allows the use of `localhost` for accessing services inside the cluster.
        - Note that the `tunnel` or `service` commands of Minikube run as background processes to keep the connection from local machine to cluster open. Run them in a different terminal window.