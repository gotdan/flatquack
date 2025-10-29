import {fhirpathToAst} from "./fhirpath-parser.js";
import {astToSql, pathsToSchema, tablesToSql} from "./ddb-sql-builder.js"
import {parseVd, extractPathsFromAst} from "./view-parser.js";
import macros from "../templates/duck-macros.js";

export function buildQuery(vd, schema, filterByResourceType, verbose, vars) {
	const parsedVd = parseVd(vd);
	if (verbose) console.log(parsedVd.path)

	const fpAst = fhirpathToAst(parsedVd.path, vd.resource, schema, vars);
	const fpSql = astToSql(fpAst).sql;

	const whereAsts = (vd.where||[]).map(w => w.path)
		.concat([filterByResourceType ? `resourceType = '${vd.resource}'` : null])
		.filter(w => !!w)
		.map(w => fhirpathToAst(w, vd.resource, schema, vars));

	const whereSql = whereAsts.map(w => {
		const whereSql = astToSql(w);
		if (whereSql.outputType.fhirType.indexOf("boolean") != 0)
			throw new Error("where path must output a boolean value");
		return `(${whereSql.sql})`;
	}).join(" and ");

	const schemaPaths = extractPathsFromAst({asts: [fpAst].concat(whereAsts)});
	const schemaSql = pathsToSchema(schemaPaths)
	const outputSql = tablesToSql(parsedVd.tables);
	return {pathSql: fpSql, schemaSql, outputSql, whereSql}
}

//TODO: consider replacing this with a full template language
export function templateToQuery(vd, schema, template, args=[], verbose, filterByResourceType, customMacros=null, vars=null) {
	//Setting filterByResourceType to btrue can only be used if the schema for the
	//elements being use is compatible between all of the resources being read
	//(e.g., element with the same names have the same structure). This is used
	//in some of the tests that mix resource types.
	
	const queryParts = buildQuery(vd, schema, filterByResourceType, verbose, vars);
	const whereSql = queryParts.whereSql ? "WHERE " + queryParts.whereSql : "";
	const schemaSql = queryParts.schemaSql ? `, columns=${queryParts.schemaSql}` : "";

	// Concatenate base macros with custom macros
	const allMacros = customMacros ? macros + '\n' + customMacros : macros;

	const templateVars = args.concat([
		["fq_input_dir", process.cwd()],
		["fq_output_dir", process.cwd()],
		["fq_where_filter", whereSql],
		["fq_sql_transform_expression", queryParts.pathSql],
		["fq_sql_input_schema", schemaSql],
		["fq_sql_flattening_cols", queryParts.outputSql.fieldSql],
		["fq_sql_flattening_tables", queryParts.outputSql.joinSql],
		["fq_vd_name", vd.name || "output"],
		["fq_vd_resource", vd.resource],
		["fq_sql_macros", allMacros]
	]);

	templateVars.forEach( v => {
		const finder = new RegExp(`\{\{\s*${v[0]}\s*\}\}`, "g");
		template = template.replace(finder, v[1]);
	})

	return template;
}