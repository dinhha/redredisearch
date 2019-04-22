/**
 * 
 * @typedef {(TEXT | NUMERIC | GEO | TAG)} FieldType
 * 
 * @typedef SchemaTypeOption
 * @type {object}
 * @property {FieldType} type - type
 * @property {boolean} sortable - sortable
 * @property {boolean} noIndex - noIndex
 * @property {number} weight - weight
 * @property {string} separator - separator
 */

class SearchSchema {
    /**
     * @param {string} key - indexing key
     * @param {Object.<string, SchemaTypeOption>} defination 
     */
    constructor(key, defination) {
        this.key = key;
        this.defination = defination;
        this.Model = SearchIndexModel;
    }

    /**
     * @returns {[string]}
     */
    getFieldArgs() {
        let fields = [];
        for (const field in this.defination) {
            if (this.defination.hasOwnProperty(field)) {
                const fieldOps = this.defination[field];
                fields.push(field);
                fields.push(fieldOps.type.name);

                if (fieldOps.weight != null && fieldOps.type == TEXT) {
                    fields.push('WEIGHT');
                    fields.push(fieldOps.weight);
                }
                if (fieldOps.separator && fieldOps.type == TAG) {
                    fields.push('SEPARATOR');
                    fields.push(fieldOps.separator);
                }

                if (fieldOps.sortable) {
                    fields.push('SORTABLE')
                }

                if (fieldOps.noIndex) {
                    fields.push('NOINDEX')
                }
            }
        }

        return fields;
    }
}

class SearchIndexModel {
    /**
     * @param {string} docId
     * @param {Object.<string, FieldType} data 
     */
    constructor(docId, data) {
        this.docId = docId;
        this.data = data;
    }

    /**
     * @returns {[string]}
     */
    getFieldArgs() {
        let fields = [];
        for (const key in this.data) {
            if (this.data.hasOwnProperty(key)) {
                const value = this.data[key];
                if (value != null) {
                    fields.push(key);
                    fields.push(value.toString());
                }
            }
        }

        return fields;
    }
}


class TEXT extends String { }
class NUMERIC extends Number { }
class GEO {
    /**
     * @param {{lat: number, lng: number}} data
     */
    constructor({lat, lng} = data) {
        this.lat = lat;
        this.lng = lng;
    }

    toString() {
        return `${this.lng} ${this.lat}`;
    }
}
class TAG {
    /**
     * @param {[string]} tags 
     */
    constructor(tags) {
        this.tags = tags;
    }

    toString() {
        return this.tags.join(',');
    }
 }


module.exports.SearchSchema = SearchSchema;
module.exports.SearchIndexModel = SearchIndexModel;
module.exports.Types = { TEXT, NUMERIC, GEO, TAG };