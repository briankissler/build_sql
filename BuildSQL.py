import pandas as pd
import numpy as np
import os



""" 
1. Read xls
2. Build Base
    Select fieldnanmes..
    from tablename

3. Build Base Pop 
    
    Select *
    from ( 
        Select *, rand() as rw
        from basePop
    ) x order by rw desc limit 10000);


3. Build Target 
    Select fieldnanmes..
    from tablename

4. Build Compare table

    select 
    tbl1.field1 as 
    ..
    tbl2.field2 as, 
    case 

    from table1 
    join table2
    on (tbl1.key1 = tbl2.key1 )


"""

#*******
#Builds the sql file
#*******
def BuildFile(fileName,fname,openT):
    file_path = fname + ".sql"

    try:
        # Attempt to open the file in exclusive creation mode ('x')
        with open(file_path, openT) as file:
            # Write something to the file
            file.write("\n" + fileName + "\n")
    except FileExistsError:
        # The file already exists
        print("File already exists.")

#*******
#Builds Base Sql
#*******
def BuildBase(s_tmp,s_tbl, s_fld, s_whr):
    fieldNames=''
    prefix = s_tbl[0]+ '.'

    """ for x in s_fld:
        fieldNames = fieldNames + prefix + x + ', ' """

    delimiter = ', '

    fieldNames = delimiter.join(s_fld)

    print(fieldNames)

    droptbl = 'drop table '+ s_tmp +'_'+s_tbl[1]+' if exists;\n\n'
    
    createTbl = 'Create Table ' + s_tmp + '_' + s_tbl[1] + ' as '
    Selecttxt = 'Select ' + fieldNames
    FromTxt = ' from '+ s_tbl[1] + ' as ' + s_tbl[0] 
    WhereTxt = ' where ' + s_whr +';\n\n'


    SqlStmt = droptbl+ createTbl + Selecttxt + FromTxt + WhereTxt

    return SqlStmt

#*******
#Builds Base Population
""" 
Select *
    from ( 
        Select *, rand() as rw
        from basePop
    ) x order by rw desc limit 10000); 
"""
#*******
def BuildSamplePop(s_tmp,s_tbl):
    bPOP = 'BasePop'
    droptbl = 'drop table '+ s_tmp +'_'+s_tbl[1]+bPOP+ ' if exists;\n\n'

    smplPop = 'Create Table '  + s_tmp +'_'+s_tbl[1]+bPOP+ ''' as select * from (
    from (
    Select *, rand() as rw
    From ''' + s_tmp + '_' + s_tbl[1] + ') x order by rw desc limit 10000);'
    
    SqlStmt = droptbl + smplPop

    return SqlStmt
    

#*******
#Builds Compare SQL
"""     select 
    tbl1.field1 as 
    ..
    tbl2.field2 as, 
    case 

    from table1 
    join table2
    on (tbl1.key1 = tbl2.key1 ) """
#*******  
def BuildCompare(p_tbl,t_tbl,c_flds):

    buildFields =''
    buildCase =''

    srcJoin = c_flds[c_flds['Source PK'] == 'Y']['SourceJoins'].values
    tgtJoin = c_flds[c_flds['Target PK'] == 'Y']['TargetJoins'].values


    print(srcJoin)

    last_index = c_flds.index[-1]  # Get the index of the last row
    
    result_string = ', \n'
    for index, row in c_flds.iterrows():
        print(row)
        if index == last_index:
            result_string = '\n'
        
        buildFields = buildFields + row['S_Concatenated'] + result_string +  row['T_Concatenated'] + result_string
        buildCase = buildCase + row['Case'] + result_string

    CompSQL = 'select ' + buildFields + buildCase + ' from ' + p_tbl + '\n join '+ t_tbl +'\, on ('+srcJoin[0] + '=' + tgtJoin[0]+' )'

    return CompSQL






#*******
#Main Section
""" 
srcWhere
sourceTbl
srcJoin
 """
#*******
df = pd.read_excel('buildTest.xlsx',header=0)

#sourceJoin = df[['Source Field','Source PK']]
#srcJoin = sourceJoin[sourceJoin['Source PK'] == 'Y']['Source Field']

#targetJoin = df[['Target Field','Target PK']]
#tgtJoin = targetJoin[targetJoin['Target PK'] == 'Y']['Target Field']

joinFields =  df[['Source Alias','Source Field','Source PK','Target Alias','Target Field','Target PK']]

joinFields['SourceJoins'] = joinFields['Source Alias'].str.cat(joinFields['Source Field'], sep='.')
joinFields['TargetJoins'] = joinFields['Target Alias'].str.cat(joinFields['Target Field'], sep='.')

#joinFields['SourceJoins'] = joinFields['SourceJoins'].str.cat(joinFields['Source Field'], sep=' as ')
#joinFields['TargetJoins'] = joinFields['TargetJoins'].str.cat(joinFields['Target Field'], sep=' as ')

# Concatenate columns A, B, and C into a new column 'Concatenated'
joinFields['S_Concatenated'] = joinFields.apply(lambda row: f"{row['SourceJoins']} as {row['Source Field']}_{row['Source Alias']}", axis=1)

# Concatenate columns A, B, and C into a new column 'Concatenated'
joinFields['T_Concatenated'] = joinFields.apply(lambda row: f"{row['TargetJoins']} as {row['Target Field']}_{row['Target Alias']}", axis=1)

joinFields['Case'] = joinFields.apply(lambda row: f"case When coalesce( {row['SourceJoins']},'') = coalesce( {row['TargetJoins']},'') then 0 else 1 end as {row['Source Field']}", axis=1)

print(joinFields)

dfFileName =  df['FileName'][0]

srcWhere =  df['Source Criteria'][0]
tgtWhere =  df['Target Criteria'][0]

BuildFile('',dfFileName,'w')

dfSourcefields = df['Source Field']

print(dfSourcefields)

sourceTbl= dfSourceTable = df[['Source Table','Source Alias']].drop_duplicates()
sourceTbl  = np.unique(df[['Source Table','Source Alias']].values)


TargetTbl= dfSourceTable = df[['Target Table','Target Alias']].drop_duplicates()
TargetTbl  = np.unique(df[['Target Table','Target Alias']].values)
dfTargetfields = df['Target Field']

#dfTarget = df[['Target Alias','Target Field','Target Table']]

#dfSourceTable = dfSourceTable.drop_duplicates()

print(sourceTbl[0] )


PopTbl=  dfFileName +'_'+ TargetTbl[1]+"BasePop" 
TgtTbl= dfFileName +'_'+ sourceTbl[0]


SourceSql = BuildBase(dfFileName,sourceTbl,dfSourcefields,srcWhere)
BuildFile(SourceSql,dfFileName,'a' )

TargetSql  = BuildBase(dfFileName,TargetTbl,dfTargetfields,tgtWhere)
BuildFile(TargetSql,dfFileName ,'a')

PopSql = BuildSamplePop(dfFileName,sourceTbl)
BuildFile(PopSql,dfFileName ,'a')

CompareSql = BuildCompare(PopTbl,TgtTbl,joinFields)
BuildFile(CompareSql,dfFileName ,'a')

#def BuildCompare(p_tbl,t_tbl,c_flds):

