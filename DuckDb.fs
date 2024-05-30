module ActionProcess


type DuckDb(dbFilePath: string) =
    let filePath = dbFilePath

    let conn =
        new DuckDB.NET.Data.DuckDBConnection($"Data Source={dbFilePath};ACCESS_MODE=READ_ONLY")

    let comm = conn.CreateCommand()
    do conn.Open()

    member DuckDb.GetSQLData<'T>(SQL: string, readerFunc: DuckDB.NET.Data.DuckDBDataReader -> 'T) =
        comm.CommandText <- SQL
        let reader = comm.ExecuteReader()

        let result =
            [| while reader.Read() do
                   readerFunc reader |]

        reader.Close()
        result

    override this.Finalize() = conn.Close()


let dbFile = "Z:/test.duckdb"
let db = DuckDb(dbFile)
let sql = "select id,class_id_map from action where name like 'A%'
                and class_id_map & (class_id_map - 1) = 0  order by time;"
let readerFunc (reader: DuckDB.NET.Data.DuckDBDataReader) =
    let id = reader.GetInt32(0)
    let class_id_map = reader.GetInt32(1)
    ( id,class_id_map)

let result = db.GetSQLData(sql, readerFunc)

// let Machine (actionSet:(int * int)array,offset) =
//     let mutable end_offset  = offset
//     for action in actionSet[offset+1..] do
//         end_offset <- end_offset + 1
//         match action with
//         | (id,class_id_map) -> 
//             if 
//     
//     
//     
//     end_offset