# elasticsearch-migration-helpers
Python scripts to ease the transition between ES2 and ES5

Compatible with Python 2 and Python 3.

## Setup

Create and activate a virtual environment with:

    python -m virtualenv venv && source venv/bin/activate

Install requirements with:

    pip install -r requirements.txt
    
   
## Scripts
 
### Bulk migration of documents from ES2 to ES5

Run with:

    (venv)$ export ES2_ORIGIN_HOST="http://localhost:9200"
    (venv)$ export ES5_TARGET_HOST="http://localhost:9206"
    (venv)$ python scripts/bulk_index_es2_to_es5.py

Given pre-existing target indices on an ES5 instance, migrate docs for the equivalent ES2 indices to the new indicies.

Assumes the target indicies have already been created, with appropriate ES5 mappings.

Uses example indicies and doc types from the GOV.UK Search API:
https://github.com/alphagov/rummager/tree/master/config/schema/indexes 

### Backup and restore for ES5

Run with:

    (venv)$ export ES2_ORIGIN_HOST="http://localhost:9200"
    (venv)$ python scripts/backup_and_restore.py [--create] [--restore]

A work-in-progress script that uses Elasticsearch's built-in snapshot feature to save backups to an ES data folder.

## Setting up local Elasticsearch instances

If you want to test the scripts locally, you'll need an ES2 and and ES5 instance.

Below is an example setup with ES5 running in a Docker container, and ES2 running inside a Vagrant virtual machine.

#### Docker container

Follow the [instructions for installing Docker](https://docs.docker.com/install/).

To start a Docker ES5 container and make it available on `localhost:9201` (with auth disabled):

    docker run -p 9201:9200 -e "discovery.type=single-node" -e "xpack.security.enabled=false" docker.elastic.co/elasticsearch/elasticsearch:5.5.3

#### Vagrant virtual machine

Set up a Vagrant VM using the ['Getting Started' steps on the GOV.UK developer docs](https://docs.publishing.service.gov.uk/manual/get-started.html).

Then, to make the Docker container available inside the VM on `localhost:9202`:

    vagrant ssh -- -R 9202:localhost:9201

Or use the Vagrantfile config (this may not work if you have the Vagrant DNS plugin installed):

    config.vm.network :forwarded_port, guest: 9202, host: 9201

Any apps or scripts running inside the Vagrant VM will now be able to access both ES instances.

For example, in the GOV.UK Rummager app, add the following config to point to the ES5 Docker container:

    rummager/elasticsearch.yml:
	base_uri: <%= ENV["ELASTICSEARCH_URI"] || 'http://localhost:9202' %>

    rummager/lib/services.rb:
    def self.elasticsearch(hosts: ENV['ELASTICSEARCH_HOSTS'] || 'http://localhost:9202', timeout: 5, retry_on_failure: false)

#### Monitoring the instances

Access info about either ES cluster from your host machine by supplying the appropriate port to:

    curl http://localhost:<PORT>/_cat/indices

Alternative, you can use the [Elasticsearch head Chrome extension](https://chrome.google.com/webstore/detail/elasticsearch-head/ffmkiejjmecolpfloofpjologoblkegm) to view the instances.

For example, with the GOV.UK Rummager app running on a Vagrant VM:

    http://rummager.dev.gov.uk:9200/

The Docker container ES instance will be available on port 9201:

    http://localhost:9201/
