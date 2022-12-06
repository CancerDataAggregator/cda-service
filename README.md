
# cda-service
This repository started as a clone of the [kernel-service-poc](https://github.com/DataBiosphere/kernel-service-poc) project.

## Sonarqube Static Code Analysis
Clicking on the following image will take you to the CDA Sonarqube code analysis dashboard.
<br />
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=CancerDataAggregator_cda-service&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=CancerDataAggregator_cda-service)

## Getting Started (macOS)

Building and running locally requires JDK 11 and gradle. On a Mac, you can use [brew](https://brew.sh/)
to install these. 

```bash
brew install openjdk@11
brew install gradle
```

After this add the path to `openjdk@11` into your login script e.g. `export PATH="/usr/local/opt/openjdk@11/bin:$PATH"`

### Build and run tests

```bash
./gradlew test
```

The end of the test output should read something like:
```
BUILD SUCCESSFUL in 8s
6 actionable tasks: 6 executed
```

### Run the server

Running the server locally requires three environment variables. These can be set on the command line:

```bash
./gradlew bootRun
```

Accessing BigQuery requires credentials. If the credentals are stored in a file called 
`bq-credentials.json`, you can start the service as follows:

```bash
GOOGLE_APPLICATION_CREDENTIALS=bq-credentials.json ./gradlew bootRun
```


### Testing the server

If the `bootRun` command was successful, you should see `EXECUTING` in the output. At this point the
server is running on port 8080 locally. The swagger page is at http://localhost:8080/api/swagger-ui.html.
You can test out the two endpoints using `curl`:

```bash
curl http://localhost:8080/status
```


### Example query

> Select data from TCGA-OV project, with donors over age 50 with Stage IIIC cancer

#### Request body

```
{
  "node_type": "AND",
  "l": {
    "node_type": "AND",
    "l": {
      "node_type": ">",
      "l": {
        "node_type": "column",
        "value": "ResearchSubject.Diagnosis.age_at_diagnosis"
      },
      "r": {
        "node_type": "unquoted",
        "value": "50 * 365"
      }
    },
    "r": {
      "node_type": "=",
      "l": {
        "node_type": "column",
        "value": "ResearchSubject.Specimen.associated_project"
      },
      "r": {
        "node_type": "quoted",
        "value": "TCGA-ESCA"
      }
    }
  },
  "r": {
    "node_type": "=",
    "l": {
      "node_type": "column",
      "value": "ResearchSubject.Diagnosis.tumor_stage"
    },
    "r": {
      "node_type": "quoted",
      "value": "stage iiic"
    }
  }
}
```

Curl line
```
curl -X POST "http://localhost:8080/api/v1/boolean-query/v0" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"node_type\":\"AND\",\"l\":{\"node_type\":\"AND\",\"l\":{\"node_type\":\">=\",\"l\":{\"node_type\":\"column\",\"value\":\"Diagnosis.age_at_diagnosis\"},\"r\":{\"node_type\":\"unquoted\",\"value\":\"50\"}},\"r\":{\"node_type\":\"=\",\"l\":{\"node_type\":\"column\",\"value\":\"Specimen.associated_project\"},\"r\":{\"node_type\":\"quoted\",\"value\":\"TCGA-OV\"}}},\"r\":{\"node_type\":\"=\",\"l\":{\"node_type\":\"column\",\"value\":\"Diagnosis.tumor_stage\"},\"r\":{\"node_type\":\"quoted\",\"value\":\"Stage IIIC\"}}}"
```

### Generating Python Client APIs

The OpenAPI YAML can be used to generate python client code. To do this, run the gradle 
task `buildPythonSdk`:

```shell
./gradlew buildPythonSdk
```

To push the generated code to the client code repo [cda-service-python-client](https://github.com/CancerDataAggregator/cda-service-python-client), run
the git-push script:
```shell
./misc/git-push.sh "Comment describing the change" 
```

Notes
- This will completely overwrite the previous code with the newly generated code.
- The python package version uses the openapi version (property `info.version`). Be sure to update the openapi yaml
version before generating a new python client, or the new client will have the same version.
  
## Logging

By default, log output is in JSON format to make it easier to process in stackdriver. Since this can make the log
harder to read, you can use text logging instead by set the environment variable `LOG_APPENDER` to `Console-Standard`
when debugging:

```shell
LOG_APPENDER=Console-Standard ./gradlew bootRun
```

## OpenAPI V3

The API specification in OpenAPI V3 is at src/main/resources/api/service_openapi.yaml

A swagger-ui page is available at /api/swagger-ui.html on any running instance. 
TEMPLATE: Once a service has a stable dev/alpha instance, a link to its 
swagger-ui page should go here.

## Spring Boot
We use Spring Boot as our framework for REST servers. The objective is to use a minimal set
of Spring features; there are many ways to do the same thing and we would like to constrain ourselves
to a common set of techniques.

### Configuration
We only use Java configuration. We never use XML files.

In general, we use type-safe configuration parameters as shown here: 
[Type-safe Configuration Properties](https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-external-config-typesafe-configuration-properties).
That allows proper typing of parameters read from property files or environment variables. Parameters are
then accessed with normal accessor methods. You should never need to use an `@Value` annotation.

### Initialization
When the applications starts, Spring wires up the components based on the profiles in place.
Setting different profiles allows different components to be included. This technique is used
as the way to choose the cloud platform (Google, Azure, AWS) code to include.

We use the Spring idiom of the `postSetupInitialization`, found in ApplicationConfiguration.java,
to perform initialization of the application between the point of having the entire application initialized and
the point of opening the port to start accepting REST requests.

### Annotating Singletons	
The typical pattern when using Spring is to make singleton classes for each service, controller, and DAO.	
You do not have to write the class with its own singleton support. Instead, annotate the class with	
the appropriate Spring annotation. Here are ones we use:
<ul>
<li><code>@Component</code> Regular singleton class, like a service.</li>
<li><code>@Repository</code> DAO component</li>
<li><code>@Controller</code> REST Controller</li>
<li><code>@Configuration</code> Definition of properties</li>
</ul>

### Common Annotations
There are other annotations that are handy to know about.

#### Autowiring
Spring wires up the singletons and other beans when the application is launched.
That allows us to use Spring profiles to control the collection of code that is
run for different environments. Perhaps obviously, you can only autowire singletons to each other. You cannot autowire
dynamically created objects.

There are two styles for declaring autowiring.
The preferred method of autowiring, is to put the annotation on the constructor
of the class. Spring will autowire all of the inputs to the constructor.
```
@Component
public class Foo {
    private Bar bar;
    private Fribble fribble;

    @Autowired
    public Foo(Bar bar, Fribble fribble) {
        this.bar = bar;
        this.foo = foo;
    }
```
Spring will pass in the instances of Bar and Fribble into the constructor.
It is possible to autowire a specific class member, but that is rarely necessary:
```
@Component
public class Foo {
    @Autowired
    private Bar bar;
```

#### REST Annotations
<ul>
<li><code>@RequestBody</code> Marks the controller input parameter receiving the body of the request</li>
<li><code>@PathVariable("x")</code> Marks the controller input parameter receiving the parameter <code>x</code></li>
<li><code>@RequestParam("y")</code> Marks the controller input parameter receiving the query parameter<code>y</code></li>
</ul>

#### JSON Annotations
We use the Jackson JSON library for serializing objects to and from JSON. Most of the time, you don't need to 
use JSON annotations. It is sufficient to provide setter/getter methods for class members
and let Jackson figure things out with interospection. There are cases where it needs help
and you have to be specific.

The common JSON annotations are:
<ul>
<li><code>@JsonValue</code> Marks a class member as data that should be (de)serialized to(from) JSON.
You can specify a name as a parameter to specify the JSON name for the member.</li>
<li><code>@JsonIgnore</code>  Marks a class member that should not be (de)serialized</li>
<li><code>@JsonCreator</code> Marks a constructor to be used to create an object from JSON.</li>
</ul>

For more details see [Jackson JSON Documentation](https://github.com/FasterXML/jackson-docs)

## Main Code Structure
This section explains the code structure of the template. Here is the directory structure:
```
/src
  /main
    /java
      /bio/terra/TEMPLATE
        /app
          /configuration
          /controller
        /common
          /exception
        /service
    /resources
```
<ul>
<li><code>/app</code> For the top of the application, including Main and the StartupInitializer</li>
<li><code>/app/configuration</code> For all of the bean and property definitions</li>
<li><code>/app/controller</code> For the REST controllers. The controllers typically do very little.
They invoke a service to do the work and package the service output into the response. The
controller package also defines the global exception handling.</li>
<li><code>/common</code> For common models and common exceptions; for example, a model that is 
shared by more than one service.</li>
<li><code>/common/exception</code> The template provides abstract base classes for the commonly
used HTTP status responses. They are all based on the ErrorReportException that provides the
explicit HTTP status and "causes" information for our standard ErrorReport model.</li>
<li><code>/service</code> Each service gets a package within. We handle cloud-platform specializations
within each service.</li>
<li><code>/resources</code> Properties definitions, database schema definitions, and the REST API definition</li>
</ul>

## Test Structure
Test methods are currently one of two kinds of tests. A unit test, which tests an individual
method in a class, or a Mock MVC test, which uses mocked services to test a specific endpoint.

Future tests could include integration tests, which would use endpoints to call into real 
(not mocked) services.

## Deployment
### On commit to master
When a commit is merged to master, the [master_push workflow](https://github.com/CancerDataAggregator/cda-service/blob/master/.github/workflows/master_push.yml) is triggered.

It has two jobs. The first is in this repo: incrementing the tag, building the Docker image, and pushing it to GCR. The second reaches out to Broad DevOps, recording the new version in their systems [here](https://beehive.dsp-devops.broadinstitute.org/charts/cancerdata/app-versions).

### Deploying to Broad's environments

To deploy a version Broad's dev environment, visit [this page](https://beehive.dsp-devops.broadinstitute.org/environments/dev/chart-releases/cancerdata/change-versions), supply a new value under "Specify App Version" -> "Set Exact Version", and scroll down to hit "Calculate and Preview". The "Apply" button on the next page will do the deployment to dev.

> **Info**
> Once deployed to Broad's dev environment, that version will be automatically promoted through our environments until finally being deployed to production alongside the usually-weekly monolith rollout. Manual deployment to production is also available ([hotfix document here](https://docs.google.com/document/d/1B9iSfAo8eaFShONLwXHgno2Gm7EHx52tNbCF4xLcHvM/edit)). Feel free to reach out to [Broad's #dsp-devops-champions](https://broadinstitute.slack.com/archives/CADM7MZ35) with any questions.

### Using cloud code and skaffold

Once you have deployed to GKE, if you are developing on the API it might be useful to update the API container image
without having to go through a full re-deploy of the Kubernetes namespace. CloudCode for IntelliJ makes this simple.
Code for local development lives in the `local-dev` directory.
First install [skaffold](https://github.com/GoogleContainerTools/skaffold) and [helm](https://helm.sh/)

    brew install skaffold helm

Next, [enable the CloudCode plugin for IntelliJ](https://cloud.google.com/code/docs/intellij/quickstart-IDEA).

Finally, run `local-dev/setup_local_env.sh <your dev environment name>`. This is a small script that clones the Terra
[helm charts](https://github.com/broadinstitute/terra-helm) and [values definitions](https://github.com/broadinstitute/terra-helmfile), then sets up your local skaffold.yaml file.

Then you should be able to either `Deploy to Kubernetes` or `Develop on Kubernetes` from the run configurations menu.
