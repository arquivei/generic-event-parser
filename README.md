### Generic Event Parser

This project is a Google Dataflow pipeline that process generic JSON messages from Google PubSub or Apache Kafka and writes it parsed to Google BigQuery.

#### How does it work?

The pipeline will read JSON messages. The expected format is something like:

```json
{
  "Id": "an-ulid-string",
  "Source": "source-system-that-produced-this-message",
  "Type": "an-awesome-event-was-produced",
  "SchemaVersion": 1,
  "DataVersion": 1,
  "Data": {
    ...
  },
  "CreatedAt": "2019-07-17T08:57:10-03:00"
}
```

You may customize this message format but you should keep at least `Source` and `Type` fields. These two fields will be used to choose the BigQuery table where messages will be placed.

A message will be written to a BigQuery table called `source_type` (snake case). So each kind of message will be written to a different table. For example, the message `account-was-created` produced by `arquivei` will be written to a table called: `arquivei_account_was_created`.

If for some reason the message could not be parsed and written in destination table, it will be placed as a raw string on a fallback table.

The destination `dataset` should be set in project configuration.

Please refer to FAQ for more information.

#### Project Structure

Each Dataflow pipeline is kept inside a different folder in `stream/` directory:

- `kafkagenericeventparser`: Reads from Kafka and writes to BigQuery
- `pubsubgenericeventparser`: Reads from Pubsub and writes to BigQuery

The inner logic of both pipelines are the same and only the source is different.

#### Configuration

Each project has a `config/` folder. Several yaml files can be placed in there for each subproject you have.

An example yaml is kept called `example.yaml.dist`. You should customize settings for your pipelines accordingly.

For example, you may create a `dev.yaml` file containing configs for testing in development environment and a `prod.yaml` file for production environment.

The name of the yaml file without extension is the name of your `project` (prod or dev, for example).

#### Migrations

A fallback table needs to be created by each pipeline. You may use the following command for that:

```bash
./exec.sh migrate <project>
```

#### Running

To deploy your pipeline just run the following command:

```bash
./exec.sh run <project>
```

#### FAQ

- What happens to timestamps and datetimes?

Once our input data is a JSON string, we look for these data types applying some regexes to find if a string is a timestamp or datetime.
Then, *ALL TIMESTAMPS AND DATETIMES ARE CONVERTED TO A DATETIME IN AMERICA/SAO_PAULO TIME ZONE*.

- What happens when schema changes?

Every message that arrives will be processed in the following way:

1. Message content will be parsed to JSON
2. All string fields are tested for Datetime regexes, if match they are converted to America/Sao_Paulo
3. All JSON keys will be transformed to PascalCase
4. Special characters are removed from JSON keys and then are conformed to BigQuery field names pattern
5. If something went wrong in the previous steps, message is marked as fallback and will be written to fallback table. If not, proceed to step 6.
6. Message will be written to BigQuery. If a problem happens, proceed to step 7.
7. If problem is caused due to a schema change, it is detected and a update is made to the table.
8. If update succeeds, message is written to BigQuery. If a problem happens, proceed to step 9.
9. Write message in fallback table.

