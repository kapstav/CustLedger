# CustLedger

This project contains a Scala application to count provider visits and convert the results into a JSON format using Apache Spark.

## Getting Started

### Prerequisites

Before running the application, ensure you have the following software installed on your Windows machine:

- [Java JDK 8 or higher](https://www.oracle.com/java/technologies/javase-downloads.html)
- [Scala](https://www.scala-lang.org/download/)
- [Apache Spark](https://spark.apache.org/downloads.html)
- [sbt (Scala Build Tool)](https://www.scala-sbt.org/download.html)
- [Git](https://git-scm.com/download/win)

### Setting Up Environment Variables

1. **Java**: Set the `JAVA_HOME` environment variable to the JDK installation path.
   - Open Command Prompt as Administrator.
   - Run: `setx JAVA_HOME "C:\Path\To\Java\jdkX.X.X"`

2. **Scala**: Add Scala to your `PATH`.
   - Open Command Prompt as Administrator.
   - Run: `setx PATH "%PATH%;C:\Path\To\Scala\bin"`

3. **Spark**: Set the `SPARK_HOME` environment variable and add it to your `PATH`.
   - Open Command Prompt as Administrator.
   - Run: `setx SPARK_HOME "C:\Path\To\Spark"`
   - Run: `setx PATH "%PATH%;%SPARK_HOME%\bin"`

4. **sbt**: Add sbt to your `PATH`.
   - Open Command Prompt as Administrator.
   - Run: `setx PATH "%PATH%;C:\Path\To\sbt\bin"`

### Cloning the Repository

Open Command Prompt or Git Bash and run the following commands:

```sh
git clone https://github.com/kapstav/CustLedger.git
cd CustLedger
