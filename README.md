Kedro to Dataiku
## Convert Kedro project to Dataiku project in minutes
 This is a tool to enable one to deploy a Kedro (>=0.16.5) project on Dataiku DSS instance without modifying the original Kedro project at all.  

- Automatic
- Fast
- Flexible

## Features
- Create Dataiku datasets automatically based on Kedro dataset catalog
- Convert Kedro nodes into Dataiku recipes 
- Convert Kedro pipelines into Dataiku flow
- Create flow zones in Dataiku project based on Kedro pipeline-determined nodes grouping 
- Load all raw input data for the Kedro project into corresponding datasets in Dataiku proeject 
- Support PySpark through PySpark recipes in Dataiku 
- Clone source code from git repository

## Constraints on Kedro project
- All outputs (including intermediate outputs) of Kedro nodes must be either Pandas DataFrame or PySpark DataFrame.
- Inputs for Kedro nodes must be Pandas DataFrame, PySpark DataFrame, dictionary of Pandas DataFrame, or Parameters. 
- Intermediate outputs (not registered in Kedro catalog) in Kedro pipeline will also be saved into Datasets in Dataiku. That is the reason why the intermediate outputs should also be Pandas/PySpark DataFrame types. 
- If there are local data files, it is required to define the file paths in templated format (https://kedro.readthedocs.io/en/stable/kedro.config.TemplatedConfigLoader.html), with the macro named as ${data_prefix} and given value in conf/local/globals.yml. Example:
    ```sh
    ### data location
    data
      --01_raw
        --sales.csv
        
    ### In conf/base/catalog.yml:
    sales:
      <<: *csv_tab
      filepath: ${data_prefix}/01_raw/sales.csv
    
    ### in conf/local/globals.yml:
    data_prefix: data/
    ```
- In order to create Dataiku zones in flow automatically, there must be several pipeplines defined (except the __default__ pipeline) in context.pipelines. The keys of context.pipelines are used to define zones and outputs in one pipepine are placed in the corresponding zone. For example, if there are pipelines defined as the following, then we can use  ["int","primary","master","modeling"] to define the zones.

    ```sh
    return {
        "int": int,
        "primary": primary,
        "master": master,
        "modeling": modeling,
        "__default__": (
                    int
                    + primary
                    + master
                    + modeling
                ),
    }
    ```
## Installation

As the package depends on dataiku which is internal module in DSS instance, it is recommneded to install and use this package inside Dataiku DSS. 

Install it in Dataiku DSS code enviroment like any other pip packages, or install in Jupyter notebook by
```sh
%pip install kedro_to_dataiku
```

The required packages "dataiku" and "kedro" will be the ones already exist in the Dataiku DSS environment. 

Instead of installing the package, one can also upload the kernel file https://github.com/ppvastar/kedro_to_dataiku/blob/main/kedro_to_dataiku/kedro_to_dataiku.py
to Dataiku project library
```sh
lib/python
```
so that one can 
```sh
from kedro_to_dataiku import *
```
in project code.

## Usage
1. Create a managed folder in Dataiku project. Let us suppose it to be "workspace".
2. Compress (into zip) and upload the whole Kedro project root folder (containing subfolders like data, conf, src, etc) into the managed folder, and uncompress it there.
3. Open Jupyter notebook in Dataiku, follow the following steps:

* Initial set up
    ```sh
    import dataiku
    from kedro_to_dataiku import *
    
    ### the absolut path to the Kedro project root folder in Dataiku DSS filesystem.
    kedro_project_path=dataiku.Folder("workspace").get_path()+"[relative path of the kedro project root folder]"
    ### package_name: name of the folder in "[kedro project root folder]/src/" which contains "nodes" and "pipelines" subfolders
    package_name="[Kedro project package name]"
    ### set dataset connection (location). Or any other established connections (like S3) in Dataiku DSS.
    connection="filesystem_managed" [or any other established connections (like S3) in Dataiku DSS]
    ### data foramt in Dataiku dataset
    format_type="csv"
    ### define recipe type. Or use "pyspark" if want to create pyspark recipes. 
    recipe_type="python" 
    ### use source code residing in kedro_project_path+"/src". Otherwise, if True, will use source code imported as Dataiku python library -- this option will enable us to edit the soruce code residing in library.
    src_in_lib=False 
    ### a list of zones to be created. They are from the keys of context.pipelines in the Kedro project. Example: ["int","primary","master","master_ds","modeling"]. Or just keep it as None so that no zones will be created automatically.
    zone_list=None
    ### if want to load the raw input data to Dataiku datasets. 
    load_data=False
    ```
* Fast creation and clean
    ```sh
    ## fast creation and clean
    ### one command to create the projects
    create_all(kedro_project_path, package_name, connection, recipe_type,zone_list,load_data,format_type,src_in_lib)
    
    ### one command to clean the projects
    delete_all()
    ```
* Create the project step by step
    ```sh
    ### create datasets
    input_list,dataset_list=create_datasets(kedro_project_path, package_name,connection,format_type,src_in_lib)
    ### create recipes
    create_recipes(kedro_project_path, package_name,recipe_type,src_in_lib)
    ### create zones
    create_zones(zone_list,kedro_project_path, package_name,src_in_lib)
    ### load raw input datasets
    load_input_datasets(input_list,kedro_project_path, package_name,src_in_lib)
    ```

* Try some other tools:
    ```sh
    ### list all datastes
    act_on_project(target="dataset",cmd="list")
    ### clear data in all datastes
    act_on_project(target="dataset",cmd="clear")
    ### delete all datastes
    act_on_project(target="dataset",cmd="delete")
    
    ### return all recipes
    act_on_project(target="recipe",cmd="list")
    ### delete all recipes
    act_on_project(target="recipe",cmd="delete")
    
    ### return all zones
    act_on_project(target="zone",cmd="list")
    ### delete all zones except the "Default". Caution: do not delete this Default zone otherwise the project flow will corrupt.
    act_on_project(target="zone",cmd="delete")
    ```

4. In Dataiku, the src code in managed folder is not editable. If one want to do simple and fast edit on code within dataiku after deployment, one can import the source code to project library (https://doc.dataiku.com/dss/latest/python/reusing-code.html) which is editable. To do this, just load (one can use git) the folder in "[kedro project root folder]/src/" which contains "nodes" and "pipelines" subfolders into the lib/python path (keep the module name as the kedro package name), and then set 
    ```sh
    src_in_lib=True 
    ```
    in previouly mentioned steps.
    
    By doing so, the soruce code (nodes, pipelines, etc) in this library "lib/python/[package_name]" will be used instead of the orgin one under "[kedro project root folder]/src/[package_name]" 
    
    
5. One can also clone Kedro project from git repository to the managed folder we created previously. 
    ```sh
    git_url="[git repository URL]"
    kedro_project_path_in_git="[relative path of Kedro project root folder on git repository]"
    ### Keep existing data folder under the kedro_project_path in Dataiku managed folder
    clone_from_git(kedro_project_path,git_url,kedro_project_path_in_git)
    ```
6. When code has been edited under path lib/python/[package_name], one may want to copy it back to the managed folder (for further download or other operations). To do this, one can use copy_lib function:
    ```sh
    ### overwrite=False, module lib/python/[package_name] will be copied to a new folder: [kedro_project_path]/src/[package_name]_lib
    copy_lib(kedro_project_path,package_name,overwrite=False)
    ### overwrite=True, [kedro_project_path]/src/[package_name] in managed folder will just be overwritten with the lib/python/[package_name]
    copy_lib(kedro_project_path,package_name,overwrite=True)
    ```

