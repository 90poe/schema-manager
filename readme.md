# Schema manager
[Docs of schema registry](https://github.com/confluentinc/schema-registry)

# How to start:

```bash
go get github.com/90poe/schema-manager
go build -o ./schema_manager
```

# Examples:

## Example of schemas.csv
```csv
subject,version,ext
dev_oos_geofencing-value,4,.avsc
dev_90_ais_position-value,latest,.avsc
vessel-position-service,1,.proto
```

## Download schemas
```bash
schema_manager download --host=schema_registry_host --file=./schemas.csv --outdir=./api
```

## Register new schemas
```bash
schema_manager register --host=schema_registry_host --subject=subject_name --version=v2 --file=./api.proto
```