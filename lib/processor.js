/**
 * Module Dependencies
 */


var __ = require('underscore');
/**
 * Processes data returned from a SQL query.
 */

var Processor = module.exports = function Processor() {
    return this;
};

/*Copie du résultat des colonnnes de la BD vers les attributs des models et supression des colonnes retournés*/
Processor.prototype.synchronizeResultWithModelAndDelete = function(result, attributes) {

    result.forEach(function(record) {
        Object.keys(attributes).forEach(function(modelAttribute) {
            var columnName = attributes[modelAttribute].columnName || modelAttribute;
            record[modelAttribute] = record[columnName.toUpperCase()];
            if (__.isUndefined(record[modelAttribute])) {
                delete record[modelAttribute];
            }
            delete record[columnName.toUpperCase()];
        });
    });
    return result;
};
/*Copie du résultat des colonnnes de la BD vers les attributs des models sans suppression*/
Processor.prototype.synchronizeResultWithModel = function(result, attributes) {

    result.forEach(function(record) {
        Object.keys(attributes).forEach(function(modelAttribute) {
            var columnName = attributes[modelAttribute].columnName || modelAttribute;
            record[modelAttribute] = record[columnName.toUpperCase()];
            if (__.isUndefined(record[modelAttribute])) {
                delete record[modelAttribute];
            }
            //delete record[columnName.toUpperCase()];
        });
    });
    return result;
};




