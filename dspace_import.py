import json

import const
import migration_const
from support.logs import log
from support.dspace_proxy import rest_proxy

#global params
eperson_id = dict()
group_id = dict()
metadata_schema_id = dict()
metadata_field_id = dict()
community_id = dict()
collection_id = dict()
item_id = dict()
workspaceitem_id = dict()
metadatavalue = dict()
handle = dict()
bitstreamformat_id = dict()
primaryBitstream = dict()
bitstream2bundle = dict()
bundle_id = dict()

#functions
def read_json(file_name):
    """
    Read data from file as json.
    @param file_name: file name
    @return: data as json
    """
    x = open(migration_const.DATA_PATH + file_name)
    json_p = json.load(x)
    x.close()
    return json_p

def do_api_post(url, param, json_p, content_type='application/json'):
    """
    Insert data into database by api if they are not None.
    @param url: url for api post
    @param param: parameters for api post
    @param json_p: posted data
    @return: response from api post
    """
    url = const.API_URL + url
    response = rest_proxy.d.api_post(url, param, json_p)
    #control of responce status is missing
    log('Api post by ' + url + ' was successfully done!')
    return response

def do_api_get_one(url, id):
    """
    Get data with id from table.
    @param url: url for api get
    @param id: id of object
    @return: response from api get
    """
    url = const.API_URL + url + '/' + str(id)
    response = rest_proxy.d.api_get(url, None, None)
    #control of response status is missing
    log('Api get by ' + url + ' was successfully done!')
    return response

def do_api_get_all(url):
    """
    Get all data from table.
    @param url: url for api get
    @return: response from api get
    """
    url = const.API_URL + url
    param = {'size' : 1000}
    response = rest_proxy.d.api_get(url, param, None)
    #control of response satus is missing
    log('Api get by url ' + url + ' was successfully done!')
    return response

def convert_response_to_json(response):
    """
    Convert response to json.
    @param response: response from api call
    @return: json created from response
    """
    return json.loads(response.content.decode('utf-8'))

def read_metadata():
    """
    Read metadatavalue as json and
    convert it to dictionary with tuple key: resource_type_id and resource_id.
    """
    global metadatavalue
    metadatavalue_json = read_json('metadatavalue.json')
    for i in metadatavalue_json:
        key = (i['resource_type_id'], i['resource_id'])
        if key in metadatavalue.keys():
            metadatavalue[key].append(i)
        else:
            metadatavalue[key] = [i]

def read_handle():
    """
    Read handle as json and convert it to dictionary wth tuple key: resource_type_id and resource_type,
    where value is list of jsons.
    """
    global handle
    handle_json = read_json('handle.json')
    for i in handle_json:
        key = (i['resource_type_id'], i['resource_id'])
        if key in handle.keys():
            handle[key].append(i)
        else:
            handle[key] = [i]

def get_metadata_value(old_resource_type_id, old_resource_id):
    """
    Get metadata value for dspace object.
    """
    global metadata_field_id, metadatavalue
    result = dict()
    if not metadatavalue:
        read_metadata()
    #get all metadatavalue for object
    if (old_resource_type_id, old_resource_id) in metadatavalue:
        metadatavalue_obj = metadatavalue[(old_resource_type_id, old_resource_id)]
    else:
        return None
    if metadatavalue_obj:
        #create list of object metadata
        for i in metadatavalue_obj:
            # get metadatafield
            metadatafield_json = convert_response_to_json(
                do_api_get_one('core/metadatafields', metadata_field_id[i['metadata_field_id']]))
            # get metadataschema
            metadataschema_json = convert_response_to_json(do_api_get_one('core/metadataschemas',
                                                           metadatafield_json['_embedded']['schema']['id']))
            #define and insert key and value of dict
            key = metadataschema_json['prefix'] + '.' + metadatafield_json['element']
            value = {'value' : i['text_value'], 'language' : i['text_lang'], 'authority' : i['authority'],
                           'confidence' : i['confidence'], 'place' : i['place']}
            if metadatafield_json['qualifier'] != None:
                key += '.' + metadatafield_json['qualifier']
            if key in result.keys():
                result[key].append(value)
            else:
                result[key] = [value]
    return result

#table mapping
def import_licenses():
    """
    Import data into database.
    Mapped tables: license_label, extended_mapping, license_definitions
    """
    files = ['license_label.json', 'license_label_extended_mapping.json', 'license_definition.json']
    end_points = ['licenses/import/labels', 'licenses/import/extendedMapping', 'licenses/import/licenses']
    for f,e in zip(files, end_points):
        do_api_post(e, None, read_json(f))

    print("License_label, Extended_mapping, License_definitions were successfully imported!")

def import_registrationdata():
    """
    Import data into database.
    Mapped tables: registrationdata
    """
    json_a = read_json('registrationdata.json')
    if json_a is not None:
        for i in json_a:
            json_p = {'email' : i['email']}
            do_api_post('eperson/registrations', None, json_p)

    print("Registration data was successfully imported!")

def import_bitstreamformatregistry():
    global bitstreamformat_id
    #read all existing data from bitstrreamformatregistry
    shortDesc2Id = dict()
    bitstreamformat = convert_response_to_json(do_api_get_all('core/bitstreamformats'))['_embedded']['bitstreamformats']
    for i in bitstreamformat:
        shortDesc2Id[i['shortDescription']] = i['id']

    json_a = read_json('bitstreamformatregistry.json')
    for i in json_a:
        level = i['support_level']
        if level == 0:
            level_str = "UNKNOWN"
        elif level == 1:
            level_str = "KNOWN"
        elif level == 2:
            level_str = "SUPPORTED"
        else:
            raise Exception("error")

        json_p = {'mimetype' : i['mimetype'], 'description' : i['description'],
                     'shortDescription' : i['short_description'], 'supportLevel' : level_str,
                     'internal' : i['internal']}
        try:
            bitstreamformat_id[i['bitstream_format_id']] = convert_response_to_json(do_api_post('core/bitstreamformats', None, json_p))['id']
        except:
            bitstreamformat_id[i['bitstream_format_id']] = shortDesc2Id[i['short_description']]
            log('Bitstreamformatregistry with short description ' + i['short_description'] + ' already exists in database!')

    print("Bitstream format registry was successfully imported!")


def import_epersongroup():
    """
    Import data into database.
    Mapped tables: epersongroup
    """
    global group_id
    json_a = read_json('epersongroup.json')
    # group Administrator and Anonymous already exist
    # we need to remember their id
    existing_data = convert_response_to_json(do_api_get_all('eperson/groups'))['_embedded']['groups']
    for i in existing_data:
        if i['name'] == 'Anonymous':
            group_id[0] = [i['id']]
        if i['name'] == 'Administrator':
            group_id[1] = [i['id']]
    for i in json_a:
        id = i['eperson_group_id']
        # group Administrator and Anonymous already exist
        # group is created with dspace object too
        if id != 0 and id != 1 and id not in group_id:
            #get group metadata
            metadata_group = get_metadata_value(6, i['eperson_group_id'])
            name = metadata_group['dc.title'][0]['value']
            del metadata_group['dc.title']
            #the group_metadata contains the name of the group
            json_p = {'name': name, 'metadata' : metadata_group}
            group_id[i['eperson_group_id']] = [convert_response_to_json(do_api_post('eperson/groups', None, json_p))['id']]

    print("Eperson group was successfully imported!")

def import_eperson():
    """
    Import data into database.
    Mapped tables: eperson, metadatavalue
    """
    global eperson_id
    json_a = read_json('eperson.json')
    for i in json_a:
        metadata = get_metadata_value(7, i['eperson_id'])
        json_p = {'selfRegistered': i['self_registered'], 'requireCertificate' : i['require_certificate'],
                  'netid' : i['netid'], 'canLogIn' : i['can_log_in'], 'lastActive' : i['last_active'],
                  'email' : i['email'], 'password' : i['password']}
        if metadata is not None:
            json_p['metadata'] = metadata
        eperson_id[i['eperson_id']] = convert_response_to_json(do_api_post('eperson/epersons', None, json_p))['id']

    print("Eperson was successfully imported!")

def import_group2group():
    """
    Import data into database.
    Mapped tables: group2group
    """
    global group_id
    json_a = read_json('group2group.json')
    if json_a is not None:
        for i in json_a:
            do_api_post('clarin/eperson/groups/' + group_id[i['parent_id']][0] + '/subgroups', None,
                        const.API_URL + 'eperson/groups/' + group_id[i['child_id']][0])
    print("Group2group was successfully imported!")

def import_group2eperson():
    """
    Import data into database.
    Mapped tables: epersongroup2eperson
    """
    global group_id, eperson_id
    json_a = read_json('epersongroup2eperson.json')
    for i in json_a:
        do_api_post('clarin/eperson/groups/' + group_id[i['eperson_group_id']][0] + '/epersons', None,
                    const.API_URL + 'eperson/groups/' + eperson_id[i['eperson_id']])
    print("Epersongroup2eperson was successfully imported!")

def import_metadataschemaregistry():
    """
    Import data into database.
    Mapped tables: metadataschemaregistry
    """
    global metadata_schema_id
    # get all existing data from database table
    existing_data = convert_response_to_json(do_api_get_all('core/metadataschemas'))['_embedded']['metadataschemas']
    json_a = read_json('metadataschemaregistry.json')
    for i in json_a:
        json_p = {'namespace' : i['namespace'], 'prefix' : i['short_id']}
        #prefix has to be unique
        try:
            metadata_schema_id[i['metadata_schema_id']] = convert_response_to_json(do_api_post('core/metadataschemas', None, json_p))['id']
        except:
            for j in existing_data:
                if j['prefix'] == i['short_id']:
                    metadata_schema_id[i['metadata_schema_id']] = j['id']
                    break
    print("MetadataSchemaRegistry was successfully imported!")

def import_metadatafieldregistry():
    """
    Import data into database.
    Mapped tables: metadatafieldregistry
    """
    global metadata_schema_id, metadata_field_id
    existing_data = convert_response_to_json(do_api_get_all('core/metadatafields'))['_embedded']['metadatafields']
    json_a = read_json('metadatafieldregistry.json')
    for i in json_a:
        json_p = {'element' : i['element'], 'qualifier' : i['qualifier'], 'scopeNote' : i['scope_note']}
        param = {'schemaId': metadata_schema_id[i['metadata_schema_id']]}
        #element and qualifier have to be unique
        try:
            metadata_field_id[i['metadata_field_id']] = convert_response_to_json(do_api_post('core/metadatafields', param, json_p))['id']
        except:
            for j in existing_data:
                if j['element'] == i['element'] and j['qualifier'] == i['qualifier']:
                    metadata_field_id[i['metadata_field_id']] = j['id']
                    break
    print("MetadataFieldRegistry was successfully imported!")


def import_community():
    """
    Import data into database.
    Mapped tables: community, community2community, metadatavalue, handle
    """
    global group_id, metadatavalue, handle, community_id
    if not handle:
        read_handle()

    json_comm = read_json('community.json')
    json_comm2comm = read_json('community2community.json')
    parent = dict()
    child = dict()
    if json_comm2comm is not None:
        for i in json_comm2comm:
            parent_id = i['parent_comm_id']
            child_id = i['child_comm_id']
            if parent_id in parent.keys():
                parent[parent_id].append(child_id)
            else:
                parent[parent_id] = [child_id]
            if child_id in child.keys():
                child[child_id].append(parent_id)
            else:
                child[child_id] = parent_id

    counter = 0
    while json_comm:
        json_p = {}
        #process community only when:
        #comm is not parent and child
        #comm is parent and not child
        #parent comm exists
        #else process it later
        i = json_comm[counter]
        i_id = i['community_id']
        if (i_id not in parent.keys() and i_id not in child.keys()) or i_id not in child.keys() or child[i_id] in community_id.keys():
            # resource_type_id for community is 4
            if (4, i['community_id']) in handle:
                handle_comm = handle[(4, i['community_id'])][0]
                json_p = {'handle': handle_comm['handle']}
            metadatavalue_comm = get_metadata_value(4, i['community_id'])
            if metadatavalue_comm is not None:
                json_p = {'metadata': metadatavalue_comm}
            # create community
            parent_id = None
            if i_id in child:
                parent_id = {'parent' : community_id[child[i_id]]}
            resp_community_id = convert_response_to_json(do_api_post('core/communities', parent_id, json_p))['id']
            community_id[i['community_id']] = resp_community_id

            # create admingroup
            if i['admin'] != None:
                group_id[i['admin']] = [convert_response_to_json(
                    do_api_post('core/communities/' + resp_community_id + '/adminGroup', None, {}))['id']]
            del json_comm[counter]
        else:
            counter += 1
        if counter == len(json_comm):
            counter = 0

    print("Community and Community2Community were successfully imported!")

def import_collection():
    """
    Import data into database.
    Mapped tables: collection, community2collection, metadatavalue, handle
    """
    global group_id, metadatavalue, handle, commnity_id, collection_id
    if not handle:
        read_handle()
    json_a = read_json('collection.json')

    comm_2_coll_json = read_json('community2collection.json')
    coll2comm = dict()
    for i in comm_2_coll_json:
        coll2comm[i['collection_id']] = i['community_id']

    #because the role DEFAULT_READ is without old group id in collection
    coll2group = dict()
    metadata_json = read_json('metadatavalue.json')

    for i in metadata_json:
        if i['resource_type_id'] == 6 and 'COLLECTION_' in i['text_value'] and '_DEFAULT_READ' in i['text_value']:
            text = i['text_value']
            positions = [ind for ind, ch in enumerate(text) if ch == '_']
            coll2group[int(text[positions[0] + 1 : positions[1]])] = i['resource_id']

    for i in json_a:
        json_p = {}
        metadata_col = get_metadata_value(3, i['collection_id'])
        if metadata_col is not None:
            json_p['metadata'] = metadata_col
        if (3, i['collection_id']) in handle:
            handle_col = handle[(3, i['collection_id'])][0]
            json_p['handle'] = handle_col['handle']
        #missing submitter and logo
        params = {'parent' : community_id[coll2comm[i['collection_id']]]}
        coll_id = convert_response_to_json(do_api_post('core/collections', params, json_p))['id']
        collection_id[i['collection_id']] = coll_id
        #import logo


        #greate group
        #template_item_id, workflow_step_1, workflow_step_3, admin are not implemented, because they are null in all data
        if i['workflow_step_2']:
            group_id[i['workflow_step_2']] = [convert_response_to_json(do_api_post('core/collections/' + coll_id + '/workflowGroups/editor', None, {}))['id']]
        if i['submitter']:
            group_id[i['submitter']] = [convert_response_to_json(do_api_post('core/collections/' + coll_id + '/submittersGroup', None, {}))['id']]
        if i['collection_id'] in coll2group:
            group_id[coll2group[i['collection_id']]] = [convert_response_to_json(do_api_post('core/collections/' + coll_id + '/bitstreamReadGroup', None, {}))['id']]
            group_id[coll2group[i['collection_id']]].append(convert_response_to_json(do_api_post('core/collections/' + coll_id + '/itemReadGroup', None, {}))['id'])

    print("Collection and Community2collection were successfully imported!")

#TODO
#Why are ze not using metadata???
def import_item():
    """
    Import data into database.
    Mapped tables: item, collection2item,workspaceitem, cwf_workflowitem, metadata, handle
    """
    global workspaceitem_id
    #create dict from items by item id
    json_a = read_json("item.json")
    items = dict()
    for i in json_a:
        items[i['item_id']] = i

    #create item and workspaceitem
    json_a = read_json("workspaceitem.json")
    if json_a is not None:
        for i in json_a:
            item = items[i['item_id']]
            import_workspaceitem(item, i['collection_id'], i['multiple_titles'], i['published_before'], i['multiple_files'],
                                 i['stage_reached'], i['page_reached'])
            del items[i['item_id']]

    print("Workspaceitem was successfully imported!")

    #create workflowitem
    #workflowitem is created from workspaceitem
    #-1, because the workflowitem doesn't contain this attribute
    json_a = read_json('workflowitem.json')
    if json_a is not None:
        for i in json_a:
            item = items[i['item_id']]
            import_workspaceitem(item, i['collection_id'], i['multiple_titles'], i['published_before'], i['multiple_files'], -1, -1)
            #create workflowitem from created workspaceitem
            params = {'id' : str(workspaceitem_id[i['item_id']])}
            do_api_post('clarin/import/workflowitem', params, None)
            del items[i['item_id']]

    print("Cwf_workflowitem was successfully imported!")

    rest_proxy.reauthenticated()
    counter = 0
    #create other items
    for i in items.values():
        json_p = {'discoverable': i['discoverable'], 'inArchive': i['in_archive'],
                  'lastModified': i['last_modified'], 'withdrawn' : i['withdrawn']}
        metadata_item = get_metadata_value(2, i['item_id'])
        if metadata_item is not None:
            json_p['metadata'] = metadata_item
        if i['item_id'] in handle:
            json_p['handle'] = handle[(2, i['item_id'])]
        params = {'owningCollection': collection_id[i['owning_collection']],
                  'epersonUUID': eperson_id[i['submitter_id']]}
        #we have to do reauthorization after some time
        if counter % 500 == 0:
            rest_proxy.reauthenticated()
        counter += 1
        response = do_api_post('clarin/import/item', params, json_p)
        response_json = convert_response_to_json(response)
        item_id[i['item_id']] = response_json['id']
    print("Item and Collection2item were successfully imported!")

def import_workspaceitem(item, owningCollectin, multipleTitles, publishedBefore, multipleFiles, stagereached, pageReached):
    global workspaceitem_id, item_id, collection_id, eperson_id

    json_p = {'discoverable': item['discoverable'], 'inArchive': item['in_archive'],
              'lastModified': item['last_modified'], 'withdrawn' : item['withdrawn']}
    metadata_item = get_metadata_value(2, item['item_id'])
    if metadata_item is not None:
        json_p['metadata'] = metadata_item

    if item['item_id'] in handle:
        json_p['handle'] = handle[(2, item['item_id'])]
        # the params are workspaceitem attributes
    params = {'owningCollection': collection_id[owningCollectin],
              'multipleTitles': multipleTitles,
              'publishedBefore': publishedBefore,
              'multipleFiles': multipleFiles, 'stageReached': stagereached,
              'pageReached': pageReached,
              'epersonUUID': eperson_id[item['submitter_id']]}
    id = convert_response_to_json(do_api_post('clarin/import/workspaceitem', params, json_p))['id']
    workspaceitem_id[item['item_id']] = id
    item_id[item['item_id']] = convert_response_to_json(rest_proxy.d.api_get(const.API_URL + 'clarin/import/' + str(id) + "/item", None, None))['id']

def import_bundle():
    """
    Import data into database.
    Mapped tables: item2bundle, bundle, bitstream, bundle2bitstream
    """
    global item_id, handle, bundle_id
    #load item2bundle into dict
    json_a = read_json("item2bundle.json")
    item2bundle = dict()
    for i in json_a:
        if i['item_id'] in item2bundle:
            item2bundle[i['item_id']].append(i['bundle_id'])
        else:
            item2bundle[i['item_id']] = [i['bundle_id']]

    #load bundles and map bundles to their primary bitstream ids
    json_a = read_json("bundle.json")
    for i in json_a:
        if i['primary_bitstream_id'] is not None:
            primaryBitstream[i['bundle_id']] = i['primary_bitstream_id']

#zmena
    counter = 0
    #import bundle without primary bitstream id
    for item in item2bundle.items():
        for bundle in item[1]:
            json_p = dict()
            #zmena
            metadata_bundle = get_metadata_value(1, bundle)
            # if metadata_bundle is not None:
            #     json_p['metadata'] = metadata_bundle
            #     json_p['name'] = metadata_bundle['dc.title'][0]['value']
            # else:
            json_p['name'] = counter
            counter+=1
            # if bundle_id in handle:
            #     json_p['handle'] = handle[(1, bundle_id)]
            bundle_id[bundle] = convert_response_to_json(do_api_post('core/items/' + str(item_id[item[0]]) + "/bundles", None, json_p))['uuid']

    print("Bundle and Item2Bundle were successfully imported!")

def import_bitstream():
    global bitstreamformat_id, handle, primaryBitstream, bitstream2bundle
    #load bundle2bitstream
    json_a = read_json("bundle2bitstream.json")
    for i in json_a:
        bitstream2bundle[i['bitstream_id']] = i['bundle_id']

    #load and import bitstreams
    json_a = read_json("bitstream.json")
    for i in json_a:
        json_p = dict()
        #zmena
        # metadata_bitstream = get_metadata_value(0, i['bitstream_id'])
        # if metadata_bitstream is not None:
        #     json_p['metadata'] = metadata_bitstream
        # if i['bitstream_id'] in handle:
        #     json_p['handle'] = handle[(0, i['bitstream_id'])]
        json_p['sizeBytes'] = i['size_bytes']
        json_p['checkSum'] = {'checkSumAlgorithm': i['checksum_algorithm'], 'value': i['checksum']}
        params = {'internal_id': i['internal_id'],
                  'storeNumber': i['store_number'],
                  'bitstreamFormat': bitstreamformat_id[i['bitstream_format_id']],
                  'deleted': i['deleted'],
                  'sequenceId': i['sequence_id'],
                  'primaryBitstream': bitstream2bundle[i['bitstream_id']] in primaryBitstream and primaryBitstream[bitstream2bundle[i['bitstream_id']]] == i['bitstream_id']}
        convert_response_to_json(do_api_post('clarin/import/core/bundles/' + str(bundle_id[bitstream2bundle[i['bitstream_id']]]) + "/bitstreams", params, json_p))

    print("Bitstream and bundle2bitstream were successfully imported!")

def import_handle_with_url():
    """
    Import handles into database with url.
    Other handles are imported by dspace objects.
    Mapped table: handles
    """
    global handle
    #if handle is empty, read handle file as json
    if not handle:
        read_handle()
    #handle with defined url has key (None, None)
    if (None, None) in handle:
        handles_url = handle[(None, None)]
        for i in handles_url:
            json_p = {'handle': i['handle'], 'url': i['url']}
            do_api_post('core/handles', None, json_p)

    print("Handles with url were successfully imported!")

def import_epersons_and_groups():
    """
    Import part of dspace: epersons and groups.
    """
    import_registrationdata()
    import_epersongroup()
    import_group2group()
    import_eperson()
    import_group2eperson()

def import_metadata():
    """
    Import part of dspace: metadata
    """
    import_metadataschemaregistry()
    import_metadatafieldregistry()

def import_hierarchy():
    """
    Import part of dspace: hierarchy
    """
    import_community()
    import_collection()

#call
print("Data migraton started!")
import_licenses()
import_bitstreamformatregistry()
import_handle_with_url()
#you have to call together
import_metadata()
#import hierarchy has to call before import group
import_hierarchy()
import_epersons_and_groups()
#we need to have eperson inported
import_item()
import_bundle()
import_bitstream()
print("Data migration is completed!")