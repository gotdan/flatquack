export function tablesToSql(tables) {

	const fieldSql = tables.filter(t => t.type == "field")
		.map(t => `${t.parent||"result"}.${t.fieldName}`)
		.join(", ");
	
	const joinSql = tables.map( (t,i) => {
		if (t.type == "nullEach" || t.allowNull) {
			return `LEFT JOIN UNNEST(${t.parent||"result"}.${t.name}) AS l_${i}(${t.name}) ON TRUE`;
		} else if (t.type == "each" || (t.type == "union" && !t.allowNull)) {
			return `CROSS JOIN UNNEST(${t.parent||"result"}.${t.name}) AS f_${i}(${t.name})`;
		}
		
	}).filter(t => !!t).join(" ")

	return {fieldSql, joinSql};

}

export function astToSql(node, inLambda, inputType={}) {

	function flattenSql(querySegments) {
		if (!querySegments) return;
		if (!Array.isArray(querySegments)) querySegments = [querySegments];
		//group nav path items with parens
		let inNav;
		querySegments.map( (s,i) => {
			const nextIsNav = querySegments[i+1] && querySegments[i+1].outputType && querySegments[i+1].outputType.isNav;
			if (s.outputType && s.outputType.isNav && !inNav && nextIsNav) {
				s.sql = "(" + s.sql;
				inNav = true;
			} else if (inNav && !nextIsNav) {
				s.sql = s.sql + ")";
				inNav = false;
			}
		})
		return {
			sql: querySegments.map(s => s && s.sql).filter(s => !!s).join("."),
			outputType: querySegments.at(-1).outputType
		}
	}

	if (Array.isArray(node)) {
		let prevOutputType = inputType;
		const outputSql = node.map(n => {
			const query = astToSql(n, inLambda, prevOutputType);
			//only treat first element of a navigation array as in lambda (prefixed with 'el')
			if (inLambda) inLambda = false; 
			if (query) prevOutputType = query.outputType;
			return query;
		});
		return outputSql;
	}

	let sql;
	let outputType;

	switch (node.segmentType) {

		//nodes with children
		case 'expr':
		case 'paren':
			const children = Array.isArray(node.children) ? node.children : [node.children];
			const query = flattenSql( astToSql(children, inLambda, inputType) );
			return {
				sql: node.segmentType == "paren" ? `(${query.sql})` : query.sql, 
				outputType: query.outputType
			}; 

		case 'nav':
			if (inLambda) {
				sql = `(el.${node.value})`
				outputType = {fhirType: node.type.fhirType, isArray: node.type.isArray, isNav: false}
			} else if (inputType.fhirType && inputType.isArray) {
				sql = `list_transform(el -> el.${node.value})${node.type.isArray ? ".flatten()" : ""}`;
				outputType = {fhirType: node.type.fhirType, isArray: true, isNav: false}
			} else {
				sql = node.value;
				outputType = {fhirType: node.type.fhirType, isArray: node.type.isArray, isNav: true};
			}
			return {sql, outputType}

		case 'literal':
			sql = node.type.fhirType != "dateTime" ? node.value : `(TIMESTAMP '${node.value.replace("T", " ")}')`
			return {sql, outputType: {fhirType: node.type.fhirType, isArray: false}};
		
		//and, or, add, subtract, multiply
		case 'components':
			const components = node.args.map( c => {
				return flattenSql( astToSql(c, inLambda) );
			});
			sql = components.map(c => c.sql).join(` ${node.operator} `);
			outputType = {fhirType: node.type.fhirType == "number" ? "number" : "boolean_expr", isArray: false}
			return {sql, outputType}

		//equality, inequality
		case 'comparison':
			let leftQuery = astToSql(node.args[0], inLambda, inputType);
			let leftIsArray = leftQuery.at(-1).outputType.isArray
			let rightQuery = astToSql(node.args[1], inLambda, inputType);
			let rightIsArray = rightQuery.at(-1).outputType.isArray;
			if (rightIsArray && !leftIsArray) {
				[rightQuery, leftQuery] = [leftQuery, rightQuery];
				[rightIsArray, leftIsArray] = [leftIsArray, rightIsArray];
			}

			if (!leftIsArray) {
				sql = ["(", flattenSql(leftQuery).sql, node.operator, flattenSql(rightQuery).sql, ")"].join(" ");
			} else {
				sql = ["(",
					`(${flattenSql(leftQuery).sql}).list_transform(el -> el ${node.operator} (${flattenSql(rightQuery).sql})).list_bool_and()`,
				")"].join("");
			}
			return {sql, outputType: {fhirType: "boolean_expr", isArray: false}};

		case 'this':
			return {
				sql: inLambda ? "el" : "",
				outputType: inputType
			}

		case 'fn':
			const firstArg = node.args[0] && node.args[0][0];
			// inputType ||= {fhirType:undefined}
			switch (node.name) {
				case 'slice':
					return {
						sql: `slice(${firstArg.value})`, 
						outputType: {fhirType: inputType.fhirType, isArray: false}
					};

				case 'join':
					sql = `list_aggregate('string_agg', ${(firstArg && firstArg.value) || "''"}).ifnull2('')`;
					return {sql, outputType: {fhirType: "string", isArray: false}}
				
				case 'where':
					if (inputType && inputType.isArray) {
						sql = `list_filter(el -> ${flattenSql(astToSql(firstArg, true)).sql})`;
						outputType = {fhirType: inputType.fhirType, isArray: true}
					} else if (inputType.fhirType) {
						sql = `as_list().list_filter(el -> ${flattenSql(astToSql(firstArg, true)).sql}).slice(1)`;
						outputType = {fhirType: inputType.fhirType, isArray: false}
					} else {
						sql = flattenSql(astToSql(firstArg)).sql;			
						outputType = {fhirType: "boolean_expr", isArray: false}
					}
					return {sql, outputType}

				case 'not':
					sql = inputType.isArray
						? "list_bool_and.is_false()"
						: "is_false()";
					return {sql, outputType: {isArray: false, fhirType: "boolean_expr"}}
	
				case 'exists':
					if (inputType.isArray) {
						const sqlExpr = inputType.fhirType == "boolean_expr" ? " = true" : "IS NOT NULL";
						sql = `list_filter(el -> el ${sqlExpr}).ifnull2([]).len() > 0`					
					} else {
						sql = inputType.fhirType == "boolean_expr" ? "is_true()" : "is_not_null()";
					}
					return {sql, outputType: {isArray: false, fhirType: "boolean_expr"}}

				case 'empty':
					if (inputType.isArray && inputType.fhirType != "boolean_expr") {
						sql = "ifnull2([NULL]).list_filter(el -> el IS NOT NULL).len() = 0";				
					} else if (inputType.isArray && inputType.fhirType == "boolean_expr"){
						sql = "ifnull2([false]).list_filter(el -> el = true).len() = 0";
					} else {
						sql = inputType.fhirType == "boolean_expr" ? "is_false()" : "is_null()";
					} 
					return {sql, outputType: {isArray: false, fhirType: "boolean_expr"}}

				//non-standard
				case '_splitPath':
					return inputType && inputType.isArray
						? {
							sql: `list_transform(el -> el.parse_path('/')[${firstArg.value}])`,
							outputType: {isArray: true, fhirType: "string"}
						}
						: {
							sql: `${inLambda ? "el." : ""}parse_path('/')[${firstArg.value}]`, 
							outputType: {isArray: false, fhirType: "string"}
						}

				//non-standard
				case '_col':
				case '_col_collection':
					const colName = firstArg.value;
					const colValue = node.args[1].at(-1);
					let colValueSql = flattenSql(astToSql(node.args[1], inLambda, inputType));
					
					// This validation can only really be run at runtime since a collection that happens
					// to have one value is treated as a non-collection and doesn't need the collection tag. 
					// if (node.name != "_col_collection" && colValueSql.outputType.isArray)
					// 	throw new Error("path in columns with collection set to true must return a collection");

					if (node.name == "_col_collection" && !colValueSql.outputType.isArray)
						throw new Error("path in columns with collection set to false must not return a collection");

					//if array of non-array type then slice by default (should this be a setting?)
					if (colValue.segmentType == "nav" && colValueSql.outputType.isArray && node.name !== "_col_collection") {
							colValueSql.sql += ".as_value()"
					} else if (node.name == "_col_collection") {
						colValueSql.sql += ".ifnull2([])"
					}
					return {sql: `${colName}: ${colValueSql.sql}`, outputType: colValueSql.outputType};
			
				//non-standard
				case '_forEach':
				case '_forEachOrNull':

					//TODO: error if each arg is not a col function
					const orNullSql = node.name == "_forEachOrNull" 
						? ".ifnull2([NULL])" 
						: ""
			
					if (!inputType.fhirType) {
						const cols = node.args.map(a => astToSql(a, inLambda, inputType)).map(flattenSql).map(a => a.sql).join(",");
						sql = `{${cols}}`;
						outputType = {fhirType: inputType.fhirType, isArray: false};
					} else if (inputType.fhirType && !inputType.isArray) {
						const cols = node.args.map(a => astToSql(a, true, inputType)).map(flattenSql).map(a => a.sql).join(",");
						sql = `as_list().list_transform(el -> {${cols}})${orNullSql}`;
						outputType = {fhirType: inputType.fhirType, isArray: true};
					} else {
						const cols = node.args.map(a => astToSql(a, true, inputType)).map(flattenSql).map(a => a.sql).join(",");
						sql = `${inLambda ? "el.as_list()." : ""}list_transform(el -> {${cols}})${orNullSql}`;
						outputType = {fhirType: inputType.fhirType, isArray: true};
					}
					return {sql, outputType}

				//non-standard
				case '_unionAll':
					const unions = node.args.map(a => {
						const flat = flattenSql(astToSql(a, inLambda, inputType));
						const arraySql = flat.outputType.isArray
							? `coalesce(${flat.sql}, [])`
							: `[${flat.sql}]`;
						return {
							sql: arraySql,
							outputType: {...flat.outputType, isArray: true}
						}
					});
					return {
						sql: unions.map(u => u.sql).join(" || "), 
						outputType: unions.length ? unions[0].outputType : {isArray: true}
					};

				default:
					throw(`function ${JSON.stringify(node)} not handled`)
			}

			default:
				throw(`${JSON.stringify(node)} not handled`)
	}
}

export function pathsToSchema(node, isInRoot=true) {
	if (Array.isArray(node)) {
		const schema = node.map(n => pathsToSchema(n, isInRoot)).join(", ");
		return (isInRoot) ? `{ ${schema} }` : schema; 
	}

	const arrayIndicator = node.isArray ? "[]" : "";
	let sqlType;
	if (!node.fhirType) console.log(`${JSON.stringify(node)} is of an unknown type`)
	if (node.children.length) {
		sqlType = `STRUCT(${node.children.map(c => pathsToSchema(c, false)).join(", ")})${arrayIndicator}`
	} else if (node.fhirType == "decimal") {
		sqlType = `DOUBLE${arrayIndicator}`;
	} else if (["boolean", "integer"].indexOf(node.fhirType) > -1) {
		sqlType = `${node.fhirType.toUpperCase()}${arrayIndicator}`;
	} else if (node.fhirType && node.fhirType[0] != node.fhirType[0].toUpperCase()) {
		sqlType = `VARCHAR${arrayIndicator}`;
	} else {
		sqlType = `JSON${arrayIndicator}`;
	}
	return isInRoot ? `${node.value}: '${sqlType}'` : `${node.value} ${sqlType}`
};
