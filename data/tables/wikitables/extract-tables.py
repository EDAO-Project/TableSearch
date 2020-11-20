def export(filename, min_rows, max_rows, output_filename, num_keep):
    """
    command line: python read_wiki_tables.py <filename tables> <min_rows> <max_rows> <outputfile>
    When min_rows > max_rows, we select tables *only* bigger than min_rows
    """
    # filename = args[1]
    # min_rows = int(args[2])
    # max_rows = int(args[3])
    # output_filename = args[4]


    #reader = pd.read_json(filename, compression='gzip', lines=True, chunksize=1000)

    reader = gzip.open(filename,'rt')

    print("Read file")

    df = pd.DataFrame(columns=['_id', 'numCols', 'numDataRows', 'numHeaderRows', 'numericColumns',
       'order', 'pgId', 'pgTitle', 'sectionTitle', 'tableCaption', 'tableData',
       'tableHeaders', 'tableId'])

    to_merge = []
    with_numeric = 0
    with_1header = 0
    num_columns = []
    num_lines = []
    num_numeric_columns = []
    num_to_keep =0
    
    p = re.compile(r'href=[\'"]?([^\'" >]+)')
    
    for line in tqdm(reader):
        table = json.loads(line)
        _has_numeric = False
        _has_1header = False
        if len(table['numericColumns']) >= 1:
            _has_numeric = True
            with_numeric+=1

        if table['numHeaderRows'] <= 1:
            _has_1header = True
            with_1header+=1

        if (not _has_numeric) or (not _has_1header):
            continue
            
        _keep =False
        
        with_numeric+=1
        bigger_than = table['numDataRows'] >= min_rows

        if min_rows < max_rows:
            lower_than = table['numDataRows'] <= max_rows
            _keep = bigger_than and lower_than                
        else:
            _keep = bigger_than               
            
        if not _keep:
            continue
            
            
        num_to_keep+=1
        num_lines.append(table['numDataRows'])
        num_columns.append(table['numCols'])
        num_numeric_columns.append(len(table['numericColumns']))        

        if len(to_merge) < num_keep:                
            to_merge.append(pd.json_normalize(data=table))
            
            

        _to_export = { k: table.get(k, '') for k in ['_id', 'numCols', 'numDataRows', 'pgTitle', 'pgId', 'tableCaption' ] }
        _to_export['numNumericCols'] = num_numeric_columns[-1]
        _to_export['headers'] =  []
        

        # This is a list of list insce in principle there are multiple headers
        # But we filter and stick with only one header
        for _header_cell in table['tableHeaders'][0]: 
            _to_export['headers'].append({
                'text' : _header_cell['text'],
                'isNumeric' : _header_cell['isNumeric'], # Numeric refers only to the content of the header cell
                # that is only if you have actually a number insider the header cell
                'links' : p.findall(_header_cell['tdHtmlString']) if len(_header_cell['surfaceLinks']) > 0 else []
            })
            
        _to_export['rows'] =  []
        for _row_data in table['tableData']:
            _row = []
            for _cell in _row_data:
                _row.append({
                    'text' : _cell['text'],
                    'isNumeric' : _cell['isNumeric'],
                    'links' : p.findall(_header_cell['tdHtmlString']) if len(_header_cell['surfaceLinks']) > 0 else []                    
                })            
            _to_export['rows'].append(_row)
        
        with open('output/WikiTables_{}_{}/table-{}.json'.format(min_rows, max_rows,table['_id']), 'w') as outfile:
            json.dump(_to_export, outfile)    
        

            
    
    # AVG/STD/Median/Max lines
    # Num Tables >1 NumericColum
    # Num Table == 1 Header
    # AVG/STD/Median/Max num columns
    #
    
    print("[Lines] MAX: ", np.max(num_lines), 
                  "AVG: ", np.average(num_lines), 
                  "STD: ", np.std(num_lines), 
                  "MED: ", np.median(num_lines) )
    
    print("[Cols] MAX: ", np.max(num_columns), 
                  "AVG: ", np.average(num_columns), 
                  "STD: ", np.std(num_columns), 
                  "MED: ", np.median(num_columns) )

    print("[Numeric] MAX: ", np.max(num_numeric_columns), 
                  "AVG: ", np.average(num_numeric_columns), 
                  "STD: ", np.std(num_numeric_columns), 
                  "MED: ", np.median(num_numeric_columns) )

    print("Total With Numeric: ", with_numeric,
          "Total with 1 Header: ", with_1header,
          "To export: ", num_to_keep)    
    
    df = pd.concat(to_merge)
    return df
    #print("Saving: ", len(df))
    #df.head()
    #df.to_pickle(output_filename, compression="xz") 




if __name__ == "__main__":
    tables = export('tables.json.gz', 50, 100, 'wikitableBiggerThan200.sample.pkl', 10)
