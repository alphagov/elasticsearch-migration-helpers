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

    (venv)$ python scripts/bulk_index_es2_to_es5.py

Given pre-existing target indices on an ES5 instance, migrate docs for the equivalent ES2 indices to the new indicies.

Assumes the target indicies have already been created, with appropriate ES5 mappings.

Uses example indicies and doc types from the GOV.UK Search API:
https://github.com/alphagov/rummager/tree/master/config/schema/indexes 
