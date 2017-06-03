#Kafka on Azure HDInsight

Das Repository beinhaltet alle Code Beispiele die ich in meiner Session am [Global Azure Bootcamp 2017](https://coding-club-linz.github.io/global-azure-bootcamp-2017/) in Linz (AT) bei den Live-Demos verwendet habe.

### Session Beschreibung

Apache Kafka ist eine verteilte, skalierbare und hochverfügbare Messaging Platform. Mit Kafka kann ein bereites Spektrum von Anwendungsfällen realisiert werden, weshalb es sich über die letzten Jahre als Herzstück vieler unternehmensweiter Datenarchitekturen etablieren konnte. Der Einsatz reicht von einfachen Message Queues bis hin zu echtzeitnahen Stream Processing Lösungen, ohne dabei auf andere schwergewichtige Frameworks od. Technologien des BigData Ökosystem-Dschungels angewiesen zu sein.

Nach einer kurzen Einführung zu den wichtigsten Komponenten von Kafka zeigt die Session in einem Mix aus Vortrag und Demos, wie einfach es ist, Kafka Anwendungen zu entwickeln und diese mit Kafka HDInsight in Azure bereitzustellen. Im Zuge dessen werden mögliche Anknüpfungspunkte zu anderen Azure Services diskutiert. Ebenso erfolgt eine Abgrenzung zu alternativen Umsetzungsmöglichkeiten von Datenstromanalysen in Azure.

### Demos

##### Producer
Zeigt die einfache Verwendung der Producer API auf Basis von simulierten Warenkorbinformationen (PurchaseSimulator) sowie Tweets aus der öffentlichen Twitter-API. Die Beispiele verwenden der Einfachheit halber lediglich String bzw. JSON Serialisierung, verzichten also auf AVRO in Kombination mit der Schema-Registry von Confluent.

##### Streaming
Zwei einfache Beispiele zu Kafka Streams sind ebenfalls enthalten, welche zeitfensterbasierte Aggregationen (lokaler State) sowie Top-N Counting von HashTags oder Emojis in Tweets (global State) demonstrieren. Hinweis: Zum Zeitpunkt des Vortrags war die in HDInsight verfügbare Version Kafka 0.10.0.0, weshalb diese Kafka Streams Beispiele nicht die neueste Kafka Streams API verwenden konnten.

##### Fragen? 
Kontaktiere den Sprecher/Autor unter [grahslhp@gmail.com](mailto:grahslhp@gmail.com)
