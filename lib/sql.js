var SqlString = require('./SqlString');
var _ = require('underscore');
_.str = require('underscore.string');
var utils = require('./utils');

var RESERVED_KEYWORDS = ['ACCESS', 'ACCOUNT', 'ACTIVATE', 'ADD', 'ADMIN', 'ADVISE', 'AFTER', 'ALL', 'ALL_ROWS', 'ALLOCATE', 'ALTER', 'ANALYZE', 'AND', 'ANY', 'ARCHIVE', 'ARCHIVELOG', 'ARRAY', 'AS', 'USER','ASC','AT','AUDIT','AUTHENTICATED','AUTHORIZATION','AUTOEXTEND','AUTOMATIC','BACKUP','BECOME','BEFORE','BEGIN','BETWEEN','BFILE','BITMAP','BLOB','BLOCK','BODY','BY','CACHE','CACHE_INSTANCES', 'CANCEL','CASCADE','CAST','CFILE','CHAINED','CHANGE','CHAR','CHAR_CS','CHARACTER','CHECK','CHECKPOINT','CHOOSE','CHUNK','CLEAR','CLOB','CLONE','CLOSE','CLOSE_CACHED_OPEN_CURSORS','CLUSTER','COALESCE', 'COLUMN','COLUMNS','COMMENT','COMMIT','COMMITTED','COMPATIBILITY','COMPILE','COMPLETE','COMPOSITE_LIMIT','COMPRESS','COMPUTE','CONNECT','CONNECT_TIME','CONSTRAINT','CONSTRAINTS','CONTENTS','CONTINUE', 'CONTROLFILE','CONVERT','COST','CPU_PER_CALL','CPU_PER_SESSION','CREATE','CURRENT','CURRENT_SCHEMA','CURREN_USER','CURSOR','CYCLE','DANGLING','DATABASE','DATAFILE','DATAFILES','DATAOBJNO','DATE','DBA', 'DBHIGH','DBLOW','DBMAC','DEALLOCATE','DEBUG','DEC','DECIMAL','DECLARE','DEFAULT','DEFERRABLE','DEFERRED','DEGREE','DELETE','DEREF','DESC','DIRECTORY','DISABLE','DISCONNECT','DISMOUNT','DISTINCT', 'DISTRIBUTED','DML','DOUBLE','DROP','DUMP','EACH','ELSE','ENABLE','END','ENFORCE','ENTRY','ESCAPE','EXCEPT','EXCEPTIONS','EXCHANGE','EXCLUDING','EXCLUSIVE','EXECUTE','EXISTS','EXPIRE','EXPLAIN','EXTENT', 'EXTENTS','EXTERNALLY','FAILED_LOGIN_ATTEMPTS','FALSE','FAST','FILE','FIRST_ROWS','FLAGGER','FLOAT','FLOB','FLUSH','FOR','FORCE','FOREIGN','FREELIST','FREELISTS','FROM','FULL','FUNCTION','GLOBAL', 'GLOBALLY','GLOBAL_NAME','GRANT','GROUP','GROUPS','HASH','HASHKEYS','HAVING','HEADER','HEAP','IDENTIFIED','IDGENERATORS','IDLE_TIME','IF','IMMEDIATE','IN','INCLUDING','INCREMENT','INDEX','INDEXED', 'INDEXES','INDICATOR','IND_PARTITION','INITIAL','INITIALLY','INITRANS','INSERT','INSTANCE','INSTANCES','INSTEAD','INT','INTEGER','INTERMEDIATE','INTERSECT','INTO','IS','ISOLATION','ISOLATION_LEVEL', 'KEEP','KEY','KILL','LABEL','LAYER','LESS','LEVEL','LIBRARY','LIKE','LIMIT','LINK','LIST','LOB','LOCAL','LOCK','LOCKED','LOG','LOGFILE','LOGGING','LOGICAL_READS_PER_CALL','LOGICAL_READS_PER_SESSION', 'LONG','MANAGE','MASTER','MAX','MAXARCHLOGS','MAXDATAFILES','MAXEXTENTS','MAXINSTANCES','MAXLOGFILES','MAXLOGHISTORY','MAXLOGMEMBERS','MAXSIZE','MAXTRANS','MAXVALUE','MIN','MEMBER','MINIMUM', 'MINEXTENTS','MINUS','MINVALUE','MLSLABEL','MLS_LABEL_FORMAT','MODE','MODIFY','MOUNT','MOVE','MTS_DISPATCHERS','MULTISET','NATIONAL','NCHAR','NCHAR_CS','NCLOB','NEEDED','NESTED','NETWORK','NEW', 'NEXT','NOARCHIVELOG','NOAUDIT','NOCACHE','NOCOMPRESS','NOCYCLE','NOFORCE','NOLOGGING','NOMAXVALUE','NOMINVALUE','NONE','NOORDER','NOOVERRIDE','NOPARALLEL','NOPARALLEL','NOREVERSE','NORMAL','NOSORT', 'NOT','NOTHING','NOWAIT','NULL','NUMBER','NUMERIC','NVARCHAR2','OBJECT','OBJNO','OBJNO_REUSE','OF','OFF','OFFLINE','OID','OIDINDEX','OLD','ON','ONLINE','ONLY','OPCODE','OPEN','OPTIMAL', 'OPTIMIZER_GOAL','OPTION','OR','ORDER','ORGANIZATION','OSLABEL','OVERFLOW','OWN','PACKAGE','PARALLEL','PARTITION','PASSWORD','PASSWORD_GRACE_TIME','PASSWORD_LIFE_TIME','PASSWORD_LOCK_TIME', 'PASSWORD_REUSE_MAX','PASSWORD_REUSE_TIME','PASSWORD_VERIFY_FUNCTION','PCTFREE','PCTINCREASE','PCTTHRESHOLD','PCTUSED','PCTVERSION','PERCENT','PERMANENT','PLAN','PLSQL_DEBUG','POST_TRANSACTION', 'PRECISION','PRESERVE','PRIMARY','PRIOR','PRIVATE','PRIVATE_SGA','PRIVILEGE','PRIVILEGES','PROCEDURE','PROFILE','PUBLIC','PURGE','QUEUE','QUOTA','RANGE','RAW','RBA','READ','READUP','REAL','REBUILD', 'RECOVER','RECOVERABLE','RECOVERY','REF','REFERENCES','REFERENCING','REFRESH','RENAME','REPLACE','RESET','RESETLOGS','RESIZE','RESOURCE','RESTRICTED','RETURN','RETURNING','REUSE','REVERSE','REVOKE','ROLE', 'ROLES','ROLLBACK','ROW','ROWID','ROWNUM','ROWS','RULE','SAMPLE','SAVEPOINT','SB4','SCAN_INSTANCES','SCHEMA','SCN','SCOPE','SD_ALL','SD_INHIBIT','SD_SHOW','SEGMENT','SEG_BLOCK','SEG_FILE','SELECT', 'SEQUENCE','SERIALIZABLE','SESSION','SESSION_CACHED_CURSORS','SESSIONS_PER_USER','SET','SHARE','SHARED','SHARED_POOL','SHRINK','SIZE','SKIP','SKIP_UNUSABLE_INDEXES','SMALLINT','SNAPSHOT','SOME','SORT', 'SPECIFICATION','SPLIT','SQL_TRACE','STANDBY','START','STATEMENT_ID','STATISTICS','STOP','STORAGE','STORE','STRUCTURE','SUCCESSFUL','SWITCH','SYS_OP_ENFORCE_NOT_NULL$','SYS_OP_NTCIMG$', 'SYNONYM','SYSDATE','SYSDBA','SYSOPER','SYSTEM','TABLE','TABLES','TABLESPACE','TABLESPACE_NO','TABNO','TEMPORARY','THAN','THE','THEN','THREAD','TIMESTAMP','TIME','TO','TOPLEVEL','TRACE','TRACING', 'TRANSACTION','TRANSITIONAL','TRIGGER','TRIGGERS','TRUE','TRUNCATE','TX','TYPE','UB2','UBA','UID','UNARCHIVED','UNDO','UNION','UNIQUE','UNLIMITED','UNLOCK','UNRECOVERABLE','UNTIL','UNUSABLE','UNUSED', 'UPDATABLE','UPDATE','USAGE','USE','USING','VALIDATE','VALIDATION','VALUE','VALUES','VARCHAR','VARCHAR2','VARYING','VIEW','WHEN','WHENEVER','WHERE','WITH','WITHOUT','WORK','WRITE','WRITEDOWN','WRITEUP', 'XID','YEAR','ZONE'];

var sql = {
    // Convert mysql format to standard javascript object
    normalizeSchema: function(schema,definition) {
        return _.reduce(schema, function(memo, field) {
           // console.log('definition normalize');console.log(definition);
            // Marshal mysql DESCRIBE to waterline collection semantics
            var attrName = field.COLUMN_NAME.toLowerCase();
            /*Comme oracle n'est pas sensible à la casse, la liste des colonnes retournées est differentes de celle enregistrée dans le schema, 
             ce qui pose des problèmes à waterline*/
            Object.keys(definition).forEach(function (key){
                if(attrName === key.toLowerCase()) attrName = key;
            });
            var type = field.DATA_TYPE;

            // Remove (n) column-size indicators
            type = type.replace(/\([0-9]+\)$/, '');

            memo[attrName] = {
                type: type
                        // defaultsTo: '',
                        //autoIncrement: field.Extra === 'auto_increment'
            };

            if (field.primaryKey) {
                memo[attrName].primaryKey = field.primaryKey;
            }

            if (field.unique) {
                memo[attrName].unique = field.unique;
            }

            if (field.indexed) {
                memo[attrName].indexed = field.indexed;
            }
            return memo;
        }, {});
    },
    countQuery: function(collectionName, options, tableDefs) {
        var query = 'SELECT count(*) as count from "' + collectionName + '"';
        return query += sql.serializeOptions(collectionName, options, tableDefs);
    },
    // @returns ALTER query for adding a column
    addColumn: function(collectionName, attrName, attrDef) {
        // Escape table name and attribute name
        var tableName = (collectionName);

        // sails.log.verbose("ADDING ",attrName, "with",attrDef);

        // Build column definition
        var columnDefinition = sql._schema(collectionName, attrDef, attrName);

        return 'ALTER TABLE ' + tableName + ' ADD ' + columnDefinition;
    },
    // @returns ALTER query for dropping a column
    removeColumn: function(collectionName, attrName) {
        // Escape table name and attribute name
        var tableName = (collectionName);
        attrName = (attrName);

        return 'ALTER TABLE ' + tableName + ' DROP COLUMN ' + attrName;
    },
    selectQuery: function(collectionName, options) {
        // Escape table name
        var tableName = (collectionName);

        // Build query
        var query = utils.buildSelectStatement(options, collectionName);
        return query += sql.serializeOptions(collectionName, options);
    },
    insertQuery: function(collectionName, data) {
        // Escape table name
        var tableName = (collectionName);

        // Build query
        return 'INSERT INTO ' + tableName + ' ' + '(' + sql.attributes(collectionName, data) + ')' + ' VALUES (' + sql.values(collectionName, data) + ')';
    },
    insertManyQuery: function(collectionName, dataList) {
        // Escape table name
        var tableName = (collectionName);

        var values = [];
        _.each(dataList, function(data) {
            // Prepare values
            Object.keys(data).forEach(function(value) {
                data[value] = utils.prepareValue(data[value]);
            });

            values.push('INSERT INTO ' + tableName + ' ' + '(' + sql.attributes(collectionName, data) + ')' + ' VALUES (' + sql.values(collectionName, data) + ')');
        });
        // Build query
        return values;
    },
    // Create a schema csv for a DDL query
    schema: function(collectionName, attributes) {
        return sql.build(collectionName, attributes, sql._schema);
    },
    hasAutoIncrementedPrimaryKey: function(definition) {
        var verif = false;
        Object.keys(definition).forEach(function(key) {
            if (!_.isUndefined(definition[key].autoIncrement) && definition[key].autoIncrement) {
                console.log(key + ' is incremented');
                verif = true;
            }
        });
        return verif;
    },
    _schema: function(collectionName, attribute, attrName) {
        attrName = (attrName);
        var type = sqlTypeCast(attribute.type, attrName);
        // Process PK field
        if (attribute.primaryKey) {

            // If type is an integer, set auto increment
            /* if(type === 'INT') {
             return attrName + ' ' + type + ' NOT NULL AUTO_INCREMENT PRIMARY KEY';
             }*/
            if (type === 'NUMBER') {
                //return attrName + ' ' + type + ' NOT NULL AUTO_INCREMENT PRIMARY KEY';
                return '"' + attrName + '" ' + type + ' NOT NULL PRIMARY KEY';
            }

            // Just set NOT NULL on other types
            return '"' + attrName + '" VARCHAR2(255) NOT NULL PRIMARY KEY';
        }

        // Process UNIQUE field
        var elseCase = '';
        if (attribute.unique) {
            elseCase+=' UNIQUE'
            //return attrName + ' ' + type + ' UNIQUE';
        }
        if (attribute.required) {
            elseCase+=' NOT NULL'
            //return attrName + ' ' + type + ' UNIQUE';
        }
        return '"' + attrName + '" ' + type + elseCase;
        
        

        //return attrName + ' ' + type + ' ';
    },
    // Create an attribute csv for a DQL query
    attributes: function(collectionName, attributes) {
        return sql.build(collectionName, attributes, sql.prepareAttribute);
    },
    // Create a value csv for a DQL query
    // key => optional, overrides the keys in the dictionary
    values: function(collectionName, values, key) {
        return sql.build(collectionName, values, sql.prepareValue, ', ', key);
    },
    updateCriteria: function(collectionName, values) {
        var query = sql.build(collectionName, values, sql.prepareCriterion);
        query = query.replace(/IS NULL/g, '=NULL');
        return query;
    },
    prepareCriterion: function(collectionName, value, key, parentKey) {
        // Special sub-attr case
        if (validSubAttrCriteria(value)) {
            return sql.where(collectionName, value, null, key);

        }

        // Build escaped attr and value strings using either the key,
        // or if one exists, the parent key
        var attrStr, valueStr;


        // Special comparator case
        if (parentKey) {

            attrStr = sql.prepareAttribute(collectionName, value, parentKey);
            valueStr = sql.prepareValue(collectionName, value, parentKey);

            // Why don't we strip you out of those bothersome apostrophes?
            var nakedButClean = _.str.trim(valueStr, '\'');

            if (key === '<' || key === 'lessThan')
                return attrStr + '<' + valueStr;
            else if (key === '<=' || key === 'lessThanOrEqual')
                return attrStr + '<=' + valueStr;
            else if (key === '>' || key === 'greaterThan')
                return attrStr + '>' + valueStr;
            else if (key === '>=' || key === 'greaterThanOrEqual')
                return attrStr + '>=' + valueStr;
            else if (key === '!' || key === 'not') {
                if (value === null)
                    return attrStr + ' IS NOT NULL';
                else
                    return attrStr + '<>' + valueStr;
            }
            else if (key === 'like')
                return attrStr + ' LIKE \'' + nakedButClean + '\'';
            else if (key === 'contains')
                return attrStr + ' LIKE \'%' + nakedButClean + '%\'';
            else if (key === 'startsWith')
                return attrStr + ' LIKE \'' + nakedButClean + '%\'';
            else if (key === 'endsWith')
                return attrStr + ' LIKE \'%' + nakedButClean + '\'';
            else
                throw new Error('Unknown comparator: ' + key);
        } else {
            attrStr = sql.prepareAttribute(collectionName, value, key);
            valueStr = sql.prepareValue(collectionName, value, key);

            // Special IS NULL case
            if (_.isNull(value)) {
                return attrStr + " IS NULL";
            } else
                return attrStr + "=" + valueStr;
        }
    },
    prepareValue: function(collectionName, value, attrName) {
        
        // Cast dates to SQL
        if (_.isDate(value)) {
            value = toSqlDate(value);
        }

        // Cast functions to strings
        if (_.isFunction(value)) {
            value = value.toString();
        }

        if (value.toString().indexOf('DBEXPR-') >= 0) {
            value = value.replace('DBEXPR-', '');
            return value;
        }
        // Escape (also wraps in quotes)
        return SqlString.escape(value);
    },
    prepareAttribute: function(collectionName, value, attrName) {
        return '"' + collectionName + '"."' + attrName + '"'; // (attrName);
    },
    // Starting point for predicate evaluation
    // parentKey => if set, look for comparators and apply them to the parent key
    where: function(collectionName, where, key, parentKey) {
        return sql.build(collectionName, where, sql.predicate, ' AND ', undefined, parentKey);
    },
    // Recursively parse a predicate calculus and build a SQL query
    predicate: function(collectionName, criterion, key, parentKey) {
        var queryPart = '';


        if (parentKey) {
            return sql.prepareCriterion(collectionName, criterion, key, parentKey);
        }

        // OR
        if (key.toLowerCase() === 'or') {
            queryPart = sql.build(collectionName, criterion, sql.where, ' OR ');
            return ' ( ' + queryPart + ' ) ';
        }

        // AND
        else if (key.toLowerCase() === 'and') {
            queryPart = sql.build(collectionName, criterion, sql.where, ' AND ');
            return ' ( ' + queryPart + ' ) ';
        }

        // IN
        else if (_.isArray(criterion)) {
            queryPart = sql.prepareAttribute(collectionName, null, key) + " IN (" + sql.values(collectionName, criterion, key) + ")";
            return queryPart;
        }

        // LIKE
        else if (key.toLowerCase() === 'like') {
            return sql.build(collectionName, criterion, function(collectionName, value, attrName) {
                var attrStr = sql.prepareAttribute(collectionName, value, attrName);

                attrStr = attrStr.replace(/'/g, '"');

                // TODO: Handle regexp criterias
                if (_.isRegExp(value)) {
                    throw new Error('RegExp LIKE criterias not supported by the MySQLAdapter yet.  Please contribute @ http://github.com/balderdashy/sails-mysql');
                }

                var valueStr = sql.prepareValue(collectionName, value, attrName);

                // Handle escaped percent (%) signs [encoded as %%%]
                valueStr = valueStr.replace(/%%%/g, '\\%');
                var condition = (attrStr + " LIKE " + valueStr);

                //condition = condition.replace(/'/g, '"');

                return condition;
            }, ' AND ');
        }

        // NOT
        else if (key.toLowerCase() === 'not') {
            throw new Error('NOT not supported yet!');
        }

        // Basic criteria item
        else {
            return sql.prepareCriterion(collectionName, criterion, key);
        }

    },
    serializeOptions: function(collectionName, options) {
        var queryPart = '';

        if (options.where) {
            queryPart += ' WHERE ' + sql.where(collectionName, options.where) + ' ';
            if (options.limit) {
                // Some MySQL hackery here.  For details, see:
                // http://stackoverflow.com/questions/255517/mysql-offset-infinite-rows
                //queryPart += 'AND ROWNUM < 10 ';
            }
        } else if (options.limit) {
            queryPart += ' WHERE ROWNUM <= ' + options.limit + ' ';
        } else {
            // Some MySQL hackery here.  For details, see:
            // http://stackoverflow.com/questions/255517/mysql-offset-infinite-rows
            //queryPart += 'WHERE ROWNUM < 10 ';
        }

        if (options.groupBy) {
            queryPart += 'GROUP BY ';

            // Normalize to array
            if (!Array.isArray(options.groupBy))
                options.groupBy = [options.groupBy];

            options.groupBy.forEach(function(key) {
                queryPart += key + ', ';
            });

            // Remove trailing comma
            queryPart = queryPart.slice(0, -2) + ' ';
        }

        if (options.sort) {
            queryPart += 'ORDER BY ';

            // Sort through each sort attribute criteria
            _.each(options.sort, function(direction, attrName) {

                queryPart += sql.prepareAttribute(collectionName, null, attrName) + ' ';

                // Basic MongoDB-style numeric sort direction
                if (direction === 1) {
                    queryPart += 'ASC, ';
                } else {
                    queryPart += 'DESC, ';
                }
            });

            // Remove trailing comma
            if (queryPart.slice(-2) === ', ') {
                queryPart = queryPart.slice(0, -2) + ' ';
            }
        }


        /*if (options.skip) {
         queryPart += 'OFFSET ' + options.skip + ' ';
         }*/

        return queryPart;
    },
    // Put together the CSV aggregation
    // separator => optional, defaults to ', '
    // keyOverride => optional, overrides the keys in the dictionary
    //          (used for generating value lists in IN queries)
    // parentKey => key of the parent to this object
    build: function(collectionName, collection, fn, separator, keyOverride, parentKey) {
        separator = separator || ', ';
        var $sql = '';
        _.each(collection, function(value, key) {
            $sql += fn(collectionName, value, keyOverride || key, parentKey);

            // (always append separator)
            $sql += separator;
        });

        // (then remove final one)
        return _.str.rtrim($sql, separator);
    },
	//Check if String is a Reserved keyword for Oracle
	isKeyword: function(string) {
		if (_.contains(RESERVED_KEYWORDS,string.toUpperCase())) {
			return true;
		}
		return false;
	}
};

// Cast waterline types into SQL data types
function sqlTypeCast(type, attrName) {
    type = type && type.toLowerCase();

    switch (type) {
        case 'string':
            return 'VARCHAR2(255)';

        case 'text':
        case 'array':
        case 'json':
            return 'VARCHAR2(255)';

        case 'boolean':
            return 'NUMBER(1) CHECK("' + attrName + '" IN (0,1))';

        case 'int':
        case 'integer':
            return 'NUMBER';

        case 'float':
        case 'double':
            return 'FLOAT';

        case 'date':
            return 'DATE';

        case 'datetime':
            return 'TIMESTAMP';

        case 'binary':
            return 'BLOB';

        default:
            console.error("Unregistered type given: " + type);
            return "TEXT";
    }
}



function wrapInQuotes(val) {
    return '"' + val + '"';
}

function toSqlDate(date) {

    date = date.getUTCFullYear() + '-' +
            ('00' + (date.getUTCMonth() + 1)).slice(-2) + '-' +
            ('00' + date.getUTCDate()).slice(-2) + ' ' +
            ('00' + date.getUTCHours()).slice(-2) + ':' +
            ('00' + date.getUTCMinutes()).slice(-2) + ':' +
            ('00' + date.getUTCSeconds()).slice(-2);

    return date;
}

// Return whether this criteria is valid as an object inside of an attribute
function validSubAttrCriteria(c) {
    return _.isObject(c) && (
            !_.isUndefined(c.not) || !_.isUndefined(c.greaterThan) || !_.isUndefined(c.lessThan) ||
            !_.isUndefined(c.greaterThanOrEqual) || !_.isUndefined(c.lessThanOrEqual) || !_.isUndefined(c['<']) ||
            !_.isUndefined(c['<=']) || !_.isUndefined(c['!']) || !_.isUndefined(c['>']) || !_.isUndefined(c['>=']) ||
            !_.isUndefined(c.startsWith) || !_.isUndefined(c.endsWith) || !_.isUndefined(c.contains) || !_.isUndefined(c.like));
}

module.exports = sql;
