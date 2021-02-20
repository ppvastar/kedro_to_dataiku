import sys    
import yaml
import pandas as pd
import logging
import os
from os import path

import importlib


logging.basicConfig(format='%(message)s',level=logging.INFO)

LOG = logging.getLogger(__name__)


def clone_from_git(kedro_project_path,git_url,kedro_project_path_in_git):
    import git
    import subprocess
    
    git_tmp="git_tmp"
    subprocess.run(["rm", "-rf", git_tmp])
    os.mkdir(git_tmp)
    git.Git(git_tmp).clone(git_url)
    src_dir=git_tmp+"/"+kedro_project_path_in_git+"/"
    
    try:
        subprocess.run(["rsync", "-avrc",src_dir,kedro_project_path])
    except:
        LOG.error("Failed to copy from git repository.")
        if not path.exists(src_dir):
            LOG.error(kedro_project_path_in_git+" not found")
        if not path.exists(kedro_project_path):
            LOG.error(kedro_project_path+" not found")
        
    subprocess.run(["rm", "-rf", git_tmp])
    
    
def copy_lib(kedro_project_path,package_name,overwrite=False):
    import subprocess
    import shutil
    
    lib_path=[i for i in sys.path if "project-python-libs" in i][0]+"/"+package_name

    if overwrite: 
        target_path=kedro_project_path+"/src/"+package_name
    else:
        target_path=kedro_project_path+"/src/"+package_name+"_lib"

    subprocess.run(["rm","-rf",target_path])
    shutil.copytree(lib_path, target_path)

    
    
def return_env(component,kedro_project_path, package_name,src_in_lib=False): 

    import kedro 
    from kedro.io import  DataCatalog


    with open(kedro_project_path+'/conf/local/globals.yml') as file:
        globals_conf = yaml.load(file, Loader=yaml.FullLoader)

    if not globals_conf:
        globals_conf={}
        
    data_prefix=["data_prefix","output_prefix"]    

    for item in data_prefix: 
        globals_conf.update({item:kedro_project_path+"/data"})
 
    with open(kedro_project_path+'/conf/local/globals.yml','w') as file:
        yaml.dump(globals_conf, file)  
        
    project_module=[]
    for module in sys.modules.keys():
        if package_name in module:
            project_module.append(module)
        
    for module in project_module:
        del sys.modules[module] 
        del module
        

    package_path=kedro_project_path+"/src/"
    while package_path in sys.path:
        sys.path.remove(package_path)
    
    if src_in_lib:
        if importlib.util.find_spec(package_name):
            LOG.info("Use source under "+str(importlib.util.find_spec(package_name).submodule_search_locations))
        else:
            LOG.error("Source not found in Python library")

    else:
        
        if path.exists(package_path+"/"+package_name):
            LOG.info("Use source under "+package_path)
            sys.path.insert(0,package_path)
        else:
            LOG.error(package_path+" does not exits")
  

    if kedro.__version__<='0.16.5':
        try:
            from kedro.framework.context import load_package_context

            context = load_package_context(
                    project_path=kedro_project_path, package_name=package_name
            )
        except:
            LOG.error("Kedro version too low? Try version >=0.16.5.")
    elif kedro.__version__>'0.16.5':
        try:
            from kedro.framework.context import load_context
            context = load_context(
                    project_path=kedro_project_path
            )
        except:
            LOG.error("Kedro version too new? Try version 0.17.0.")
         
    LOG.info("Project module information:")
    LOG.info(str(sys.modules[package_name]))
    
    if component=="pipeline":
        return context.pipeline
    elif component=="pipelines":
        return context.pipelines
    elif component=="catalog":
        return context.catalog
    elif component=="catalog_conf":
        return context.config_loader.get('catalog*', 'catalog*/**')
    
            

def generate_df_dict(df): 

    if "sheet_idx" in df.columns:
        df_dict=dict()
        for idx in set(df["sheet_idx"]):
            df_dict[str(idx)]=df.loc[df["sheet_idx"]==idx].drop(columns=['sheet_idx'])

        return df_dict     
    
    else:
        return df
    
    
    
def get_node(func_name,kedro_project_path, package_name,src_in_lib=False):
    pipeline=return_env("pipeline",kedro_project_path, package_name,src_in_lib)
    
    for node in pipeline.nodes:
        if ": " in str(node):
            func=str(node).split(": ")[1].split("([")[0]
        else:
            func=str(node).split("([")[0]
        if func==func_name:
            return node
        


def run_node(func_name,kedro_project_path, package_name,src_in_lib=False,write_ds=True):
    import dataiku
    
    node=get_node(func_name,kedro_project_path, package_name,src_in_lib)
    
    inputs=node.inputs
    outputs=node.outputs
    
    catalog_conf=return_env("catalog_conf",kedro_project_path, package_name,src_in_lib)
    catalog=return_env("catalog",kedro_project_path, package_name,src_in_lib)
    
    input_dict={}

    for input_item in inputs:
        
        if  'params:' in input_item:
            input_dict[input_item]=catalog.load(input_item)
        else:
            if "PyDataFrame" in dataiku.Dataset(input_item).read_metadata()['tags']:
                from pyspark.sql import SQLContext,SparkSession
                from dataiku import spark as dkuspark
    
                input_dict[input_item]=dkuspark.get_dataframe(SQLContext(SparkSession.builder.getOrCreate()), dataiku.Dataset(input_item))
            else:    
                input_df=dataiku.Dataset(input_item).get_dataframe()
                                   
                if "DictPandas" in dataiku.Dataset(input_item).read_metadata()['tags']:
                    with_null_columns=[key for key,value in dict(input_df.isna().any()).items() if value]
                    for col in with_null_columns:
                        if str(input_df[col].dtype)=='object':
                            input_df[col]=input_df[col].astype(str)
                            
                    
                    input_df=generate_df_dict(input_df)


                input_dict[input_item]=input_df

                
    res=node.run(input_dict)
    
    if write_ds==False:
        return res
    else:
        for output in outputs:
            if 'pyspark.sql.dataframe.DataFrame' in str(type(res[output])):
                from dataiku import spark as dkuspark
                dkuspark.write_with_schema(dataiku.Dataset(output), res[output])
                dataiku.Dataset(output).write_metadata({'checklists': {'checklists': []}, 'tags': ["PyDataFrame"], 'custom': {'kv': {}}})

            else:
                dataiku.Dataset(output).write_with_schema(res[output])
                
                
                
                
############### Convert Kedro Project to Dataiku Project#######################                
                
                
def act_on_project(target="dataset",cmd="list"):
    import dataiku
    
    client = dataiku.api_client()

    project=client.get_project(dataiku.default_project_key())


    if target=="dataset":
        datasets = project.list_datasets()
        if cmd=="list":
            return datasets
        elif cmd=="delete":
            for tmp_ds in datasets:
                ds=project.get_dataset(tmp_ds.name)
                ds.delete()
                LOG.info(tmp_ds.name+" deleted")
                
        elif cmd=="clear":
             for tmp_ds in datasets:
                ds=project.get_dataset(tmp_ds.name)
                ds.clear()
                LOG.info(tmp_ds.name+" cleared")
            
                
    if target=="recipe":
        recipes = project.list_recipes()
        if cmd=="list":
            return recipes
        elif cmd=="delete":
            for tmp_rp in recipes:
                rp=project.get_recipe(tmp_rp.name)
                rp.delete()
                LOG.info(tmp_rp.name+" deleted")


    if target=="zone":
        flow = project.get_flow()
        if cmd=="list":
            return flow.list_zones()
                
        elif cmd=="delete":
            for zone in flow.list_zones():
                if zone.name!='Default':
                    zone.delete()
                    LOG.info(zone.name+" deleted")

                            
                           
def change_dataset_format(format_type="csv",datasets=None):
    import dataiku
    
    client = dataiku.api_client()
    
    project=client.get_project(dataiku.default_project_key())

    
    if datasets==None:
        datasets=project.list_datasets()

    for tmp_ds in datasets:
        ds=project.get_dataset(tmp_ds)
        settings = ds.get_settings()
        settings.set_format(format_type)
        settings.save()
  
        
        
def create_datasets(kedro_project_path, package_name,connection,format_type=None,src_in_lib=False):
    import dataiku
    
    client = dataiku.api_client()

    project=client.get_project(dataiku.default_project_key())

    
    pipeline=return_env("pipeline",kedro_project_path, package_name,src_in_lib)
    
    input_list=[]
    output_list=[]

    for node in pipeline.nodes:
        input_list=input_list+node.inputs
        output_list=output_list+node.outputs

    dataset_list=list(set(input_list+output_list))
    dataset_list=[i for i in dataset_list if 'params:' not in i]

    input_list=list(set([i for i in input_list if 'params:' not in i]))
    input_list=[i for i in input_list if i not in output_list]

    
    for dataset_name in dataset_list:
        builder = project.new_managed_dataset_creation_helper(dataset_name)
        builder.with_store_into(connection, format_option_id=format_type)
        dataset = builder.create()
        LOG.info(dataset_name+" created")
        

    
    return input_list,dataset_list
    

    
def load_input_datasets(input_list,kedro_project_path, package_name,src_in_lib=False):

    import dataiku
    
    from kedro.io import  DataCatalog


    from kedro.extras.datasets.pandas import  (
    CSVDataSet,
    ParquetDataSet,
    ExcelDataSet
    )


    catalog_conf=return_env("catalog_conf",kedro_project_path, package_name,src_in_lib)

    catalog=return_env("catalog",kedro_project_path, package_name,src_in_lib)

    pydf_catalog_dict={}

    for raw in input_list:

        if catalog_conf[raw]['type']=='spark.SparkDataSet':

            if 'load_args' in catalog_conf[raw].keys():
                load_args=catalog_conf[raw]['load_args']
                if '.csv' in catalog_conf[raw]['filepath']:
                    load_args.pop('inferSchema')
                    load_args.pop('header')
            else:
                load_args=None


            if catalog_conf[raw]['file_format']=='csv':
                pydf_catalog_dict[raw]=CSVDataSet(load_args=load_args,filepath=catalog_conf[raw]['filepath'])
            elif  catalog_conf[raw]['file_format']=='parquet':
                pydf_catalog_dict[raw]=ParquetDataSet(load_args=load_args,filepath=catalog_conf[raw]['filepath'])
            else:
                LOG.warning(raw+" with format "+catalog_conf[raw]['file_format']+" is not loaded")


    pydf_catalog=DataCatalog(pydf_catalog_dict)


    for item in input_list:
        if item not in pydf_catalog_dict.keys():
            item_df=catalog.load(item)
            if type(item_df)==dict:
                consolidate_df=pd.DataFrame()
                for idx in item_df.keys():
                    part_df=item_df[idx]
                    part_df["sheet_idx"]=idx
                    consolidate_df=consolidate_df.append(part_df)
                item_df=consolidate_df
                dataiku.Dataset(item).write_metadata({'checklists': {'checklists': []}, 'tags': ["DictPandas"], 'custom': {'kv': {}}})

            
            dataiku.Dataset(item).write_with_schema(item_df)   

                
        else:
            item_df=pydf_catalog.load(item)
            
            dataiku.Dataset(item).write_with_schema(item_df)    

            dataiku.Dataset(item).write_metadata({'checklists': {'checklists': []}, 'tags': ["PyDataFrame"], 'custom': {'kv': {}}})
        
        LOG.info(item+" loaded")
    

    
    
def create_recipes(kedro_project_path, package_name,recipe_type="python",src_in_lib=False):
    import dataiku

    client = dataiku.api_client()
    
    project=client.get_project(dataiku.default_project_key())

    
    pipeline=return_env("pipeline",kedro_project_path, package_name,src_in_lib)


    for node in pipeline.nodes:
        if ": " in str(node):
            func=str(node).split(": ")[1].split("([")[0]
        else:
            func=str(node).split("([")[0]
            
        inputs=node.inputs
        inputs=[i for i in inputs if "params:" not in i]

        outputs=node.outputs

        dataset=project.get_dataset(inputs[0])

        recipe_builder = dataset.new_recipe(recipe_type)
        for input_ds in inputs[1:]:
            recipe_builder.with_input(input_ds)
        for output_ds in outputs:
            recipe_builder.with_output(output_ds) 

        recipe_builder.with_script("""
from kedro_to_dataiku import run_node

run_node('"""+func+"""','"""+kedro_project_path+"""','"""+package_name+"""',"""+str(src_in_lib)+""")"""
            )

        recipe = recipe_builder.create()
        
        settings = recipe.get_settings()
        
  #      if code_env:
            
  #          settings.set_code_env(code_env=code_env)
  #      else:

        settings.set_code_env(inherit=True)

        settings.save()

        LOG.info(recipe.name+" created")

     
    
    
def create_zones(zone_list,kedro_project_path, package_name,src_in_lib=False):
    import dataiku

    client = dataiku.api_client()

    project=client.get_project(dataiku.default_project_key())
    
    pipelines=return_env("pipelines",kedro_project_path, package_name,src_in_lib)

    zone_mapping={}
    for key in pipelines.keys():

        if key in zone_list:
            for node in pipelines[key].nodes:

                for output in node.outputs:

                    zone_mapping[output]=key
                    
                    
    for zone_name in set(zone_mapping.values()):

        flow = project.get_flow()
        zone = flow.create_zone(zone_name)
        LOG.info(zone_name+" created")

        datasets=[i for i in zone_mapping.keys() if zone_mapping[i]==zone_name]

    # First way of adding an item to a zone
        for ds_name in datasets:
            ds = project.get_dataset(ds_name)
            ds.move_to_zone(zone)
            zone.add_item(ds)

            LOG.info("***"+ds_name+" added" )
            
            
            
def create_all(kedro_project_path, package_name, connection, recipe_type,zone_list=None,load_data=True,format_type=None,src_in_lib=False):
    LOG.info("**********")
    LOG.info("***Create datasets***")
    input_list,dataset_list=create_datasets(kedro_project_path, package_name,connection,format_type,src_in_lib)
    LOG.info("**********")
    LOG.info("***Create recipes***")
    create_recipes(kedro_project_path, package_name,recipe_type,src_in_lib)

    if zone_list:
        LOG.info("**********")
        LOG.info("***Create zones***")
        create_zones(zone_list,kedro_project_path, package_name,src_in_lib)
        
    if load_data:
        LOG.info("**********")
        LOG.info("***Load input data***")
        load_input_datasets(input_list,kedro_project_path, package_name,src_in_lib)

    
def delete_all():
    LOG.info("**********")
    LOG.info("***Delete zones***")
    act_on_project(target="zone",cmd="delete")
    LOG.info("**********")
    LOG.info("***Delete datasets***")
    act_on_project(target="dataset",cmd="delete")
    