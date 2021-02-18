import dataiku
from dataiku import spark as dkuspark

import kedro 
from kedro.io import  DataCatalog

import sys
import yaml
import pandas as pd


def return_env(component,kedro_project_path, package_name): 
    

    with open(kedro_project_path+'/conf/local/globals.yml') as file:
        globals_conf = yaml.load(file, Loader=yaml.FullLoader)

    if not globals_conf:
        globals_conf={}
        
    data_prefix=["data_prefix","output_prefix"]    

    for item in data_prefix:
        globals_conf.update({item:kedro_project_path+"/data"})

    with open(kedro_project_path+'/conf/local/globals.yml','w') as file:
        yaml.dump(globals_conf, file)   


    sys.path.insert(0,kedro_project_path+"/src/")

    if kedro.__version__=='0.16.5':
        from kedro.framework.context import load_package_context

        context = load_package_context(
                project_path=kedro_project_path, package_name=package_name
        )
    elif kedro.__version__>='0.17.0':

        from kedro.framework.context import load_context
        context = load_context(
                project_path=kedro_project_path
        )


    
    if component=="pipeline":
        return context.pipeline
    elif component=="pipelines":
        return context.pipelines
    elif component=="catalog":
        return context.catalog
    elif component=="catalog_conf":
        return context.config_loader.get('catalog*', 'catalog*/**')
        
#         conf_paths = [kedro_project_path+'/conf/base', kedro_project_path+'/conf/local']
        
#         globals_dict={}
        
#         data_prefix=["data_prefix","output_prefix"]
        
#         for item in data_prefix:
#             globals_dict.update({item:kedro_project_path+"/data"})

#         config_loader=TemplatedConfigLoader(
#                     conf_paths,
#                     globals_pattern="*globals.yml",  # read the globals dictionary from project config
#                     globals_dict=globals_dict,
#                 )

 
#         #catalog_conf=context.config_loader.get('catalog*', 'catalog*/**')
        
#         catalog_conf=config_loader.get('catalog*', 'catalog*/**')
        
#         if component=="catalog_conf":
#             return catalog_conf

#         elif component=="catalog":
#             return DataCatalog(catalog_conf)
            
            

def generate_df_dict(df): 

    if "sheet_idx" in df.columns:
        df_dict=dict()
        for idx in set(df["sheet_idx"]):
            df_dict[str(idx)]=df.loc[df["sheet_idx"]==idx].drop(columns=['sheet_idx'])

        return df_dict     
    
    else:
        return df
    
    
    
def get_node(func_name,kedro_project_path, package_name):
    pipeline=return_env("pipeline",kedro_project_path, package_name)
    
    for node in pipeline.nodes:
        if ": " in str(node):
            func=str(node).split(": ")[1].split("([")[0]
        else:
            func=str(node).split("([")[0]
        if func==func_name:
            return node
        


def run_node(func_name,kedro_project_path, package_name,write_ds=True):
    
    node=get_node(func_name,kedro_project_path, package_name)
    
    inputs=node.inputs
    outputs=node.outputs
    
    catalog_conf=return_env("catalog_conf",kedro_project_path, package_name)
    catalog=return_env("catalog",kedro_project_path, package_name)
    
    input_dict={}

    for input_item in inputs:
        
        if  'params:' in input_item:
            input_dict[input_item]=catalog.load(input_item)
        else:
            if "PyDataFrame" in dataiku.Dataset(input_item).read_metadata()['tags']:
                from pyspark.sql import SQLContext,SparkSession
    
                input_dict[input_item]=dkuspark.get_dataframe(SQLContext(SparkSession.builder.getOrCreate()), dataiku.Dataset(input_item))
            else:    
                input_df=dataiku.Dataset(input_item).get_dataframe()

                for col in input_df.columns:
                    if set((input_df.apply(lambda row:type(row[col]).__name__,axis=1)))=={'float', 'str'}:
                        input_df[col]=input_df[col].astype(str)

                input_dict[input_item]=generate_df_dict(input_df)

                
    res=node.run(input_dict)
    
    if write_ds==False:
        return res
    else:
        for output in outputs:
            if 'pyspark.sql.dataframe.DataFrame' in str(type(res[output])):
                dkuspark.write_with_schema(dataiku.Dataset(output), res[output])
                dataiku.Dataset(output).write_metadata({'checklists': {'checklists': []}, 'tags': ["PyDataFrame"], 'custom': {'kv': {}}})

            else:
                dataiku.Dataset(output).write_with_schema(res[output])
                
                
                
                
                
############### Convert Kedro Project to Dataiku Project#######################                
                
                
def act_on_project(target="dataset",cmd="list"):
    
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
                print(str(tmp_ds.name)+" deleted")
                
        elif cmd=="clear":
             for tmp_ds in datasets:
                ds=project.get_dataset(tmp_ds.name)
                ds.clear()
                print(str(tmp_ds.name)+" cleared")
            
                
    if target=="recipe":
        recipes = project.list_recipes()
        if cmd=="list":
            return recipes
        elif cmd=="delete":
            for tmp_rp in recipes:
                rp=project.get_recipe(tmp_rp.name)
                rp.delete()
                print(str(tmp_rp.name)+" deleted")                
                
           
                
def change_dataset_format(format_type="csv",dataset=None):
    
    client = dataiku.api_client()
    
    project=client.get_project(dataiku.default_project_key())

    
    if dataset==None:
        for tmp_ds in project.list_datasets():
            ds=project.get_dataset(tmp_ds)
            settings = ds.get_settings()
            settings.set_format(format_type)
            settings.save()
    else:
        ds=project.get_dataset(dataset)
        settings = ds.get_settings()
        settings.set_format(format_type)
        settings.save()
        
        
def create_datasets(kedro_project_path, package_name,connection,format_type=None):
    
    client = dataiku.api_client()

    project=client.get_project(dataiku.default_project_key())

    
    pipeline=return_env("pipeline",kedro_project_path, package_name)
    
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
        print(dataset_name+" created")
        

    
    return input_list,dataset_list
    

    
def load_input_datasets(input_list,kedro_project_path, package_name):


    from kedro.extras.datasets.pandas import  (
    CSVDataSet,
    ParquetDataSet,
    ExcelDataSet
    )


    catalog_conf=return_env("catalog_conf",kedro_project_path, package_name)

    catalog=return_env("catalog",kedro_project_path, package_name)

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
                print("***Not included***")
                print(catalog_conf[raw]['file_format'])


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
            
            dataiku.Dataset(item).write_with_schema(item_df)    

                
        else:
            item_df=pydf_catalog.load(item)
            
            dataiku.Dataset(item).write_with_schema(item_df)    

            dataiku.Dataset(item).write_metadata({'checklists': {'checklists': []}, 'tags': ["PyDataFrame"], 'custom': {'kv': {}}})
        

        print(item+" loaded")
    

    
    
def create_recipes(kedro_project_path, package_name,recipe_type="pyspark"):

    client = dataiku.api_client()
    
    project=client.get_project(dataiku.default_project_key())

    
    pipeline=return_env("pipeline",kedro_project_path, package_name)


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

run_node('"""+func+"""','"""+kedro_project_path+"""','"""+package_name+"""')"""
            )

        recipe = recipe_builder.create()
        
        print(str(recipe.name)+" created")

     
    
    
def create_zone(zone_list,kedro_project_path, package_name):

    client = dataiku.api_client()

    project=client.get_project(dataiku.default_project_key())
    
    pipelines=return_env("pipelines",kedro_project_path, package_name)

    zone_mapping={}
    for key in pipelines.keys():

        if key in zone_list:
            for node in pipelines[key].nodes:

                for output in node.outputs:

                    zone_mapping[output]=key
                    
                    
    for zone_name in set(zone_mapping.values()):

        flow = project.get_flow()
        zone = flow.create_zone(zone_name)
        print("Zone "+zone_name+" created" )

        datasets=[i for i in zone_mapping.keys() if zone_mapping[i]==zone_name]

    # First way of adding an item to a zone
        for ds_name in datasets:
            ds = project.get_dataset(ds_name)
            ds.move_to_zone(zone)
            zone.add_item(ds)
            print("***"+ds_name+" added" )



