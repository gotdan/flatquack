import fhirpath from "fhirpath";

export function fhirpathToAst(path, resourceType, schema, vars) {
	const parsedPath = fhirpath.parse(path);
	return simplifyFhirPath(parsedPath, {schemaPath: resourceType}, schema, vars || {});
}

function simplifyFhirPath(node, type = null, schema = {}, vars = {}) {

	switch (node.type) {
		case undefined:
		case 'EntireExpression':
			const exprNodes = simplifyFhirPath(node.children[0], type, schema, vars);
			return {
				segmentType: "expr",
				children: Array.isArray(exprNodes) ? exprNodes : [exprNodes],
				type: Array.isArray(exprNodes) ? exprNodes.at(-1).type : exprNodes.type
			}

		case 'ParenthesizedTerm':
			const parenNodes = simplifyFhirPath(node.children[0], type, schema, vars);
			return [{
				segmentType: "paren",
				children: Array.isArray(parenNodes) ? parenNodes : [parenNodes],
				type: parenNodes.at(-1).type
			}]

		case 'AdditiveExpression':
		case 'SubtractiveExpression':
		case 'MultiplicativeExpression':
		case 'AndExpression':
		case 'OrExpression':
		case 'InequalityExpression':
		case 'EqualityExpression':
			const components = node.children.map(c => simplifyFhirPath(c, type, schema, vars));
			const exprFhirType = /Add|Sub|Mult/.test(node.type) ? "number" : "boolean"
			const isComparison = /Inequality|Equality/.test(node.type);
			return [{
				segmentType: isComparison ? "comparison" : "components",
				operator: node.terminalNodeText[0],
				args: components, isComparison,
				type: exprFhirType
			}]

		case 'InvocationExpression':
		case 'IndexerExpression':
			if (node.terminalNodeText[0] == '.' || node.terminalNodeText[0] == '[') {

				let beforeNavigation = simplifyFhirPath(node.children[0], type, schema, vars)
				let navigationNode = simplifyFhirPath(node.children[1], beforeNavigation.at(-1).type, schema, vars)

				//redo last navigation value and schema in the context of ofType param
				//this is a bit hacky
				if (navigationNode[0].name == "ofType") {
					let lastNav = beforeNavigation.slice().reverse().find(n => n.segmentType == "nav");
					const inputTypeNode = beforeNavigation.slice().reverse().find(n => n.type.fhirType);
					const targetType = navigationNode[0].args[0][0].value;
					const newValue = lastNav.value + targetType[0].toUpperCase() + targetType.slice(1);
					let segmentFhirType = resolveType(inputTypeNode ? inputTypeNode.type : type, newValue, schema);
					lastNav.type = segmentFhirType;
					lastNav.value = newValue;
					return beforeNavigation;
				}

				if (node.terminalNodeText[0] === '[' || navigationNode[0].name == "first") {
					const slice = navigationNode[0].value ? parseInt(navigationNode[0].value) + 1 : 1;
					navigationNode[0] = {
						segmentType: "fn",
						name: "slice",
						args: [[{ "segmentType": "literal", "value": slice }]],
						type: { ...beforeNavigation.at(-1).type }
					}
				}

				return beforeNavigation.concat(navigationNode);
			}

		case 'TermExpression':
		case 'InvocationTerm':
		case 'MemberInvocation':
			if (node.children.length === 1)
				return simplifyFhirPath(node.children[0], type, schema, vars);

		case 'Identifier':
			const nodeText = node.terminalNodeText[0];
			let segmentFhirType = resolveType(type, nodeText, schema);
			return [{ segmentType: "nav", value: nodeText, type: segmentFhirType }]

		case 'FunctionInvocation':
			const functionName = node.children[0].children[0].terminalNodeText[0];
			const args = (node.children[0].children[1]?.children || []).map((param) => {
				return simplifyFhirPath(param, type, schema, vars)
			});

			let outputType;

			if (functionName == "extension") {
				return [{
					segmentType: "nav",
					value: "extension",
					type: {
						isArray: true, fhirType: "Extension", schemaPath: "Extension"
					}
				}, {
					segmentType: "fn",
					name: "where",
					args: [[{
						segmentType: "comparison", operator: "=", args: [[{
							segmentType: "nav", value: "url",
							type: {
								isArray: false, fhirType: "uri",
								schemaPath: "Extension.url",
							}
						}], [{
							segmentType: "literal", value: args[0][0].value,
							type: { isArray: false, fhirType: args[0][0].type.fhirType }
						}]],
						type: { isArray: false, fhirType: "boolean" }
					}]],
					type: {
						isArray: true, fhirType: "Extension", schemaPath: "Extension"
					}
				}];
			}

			if (functionName == "getResourceKey") {
				return [{
					segmentType: "nav",
					value: "id",
					type: { isArray: false, fhirType: "string", schemaPath: "id" }
				}]
			}

			if (functionName == "getReferenceKey" && args[0]) {
				return [{
					segmentType: "nav", value: "reference",
					type: { isArray: false, fhirType: "string", schemaPath: "Reference.reference" }
				}, {
					segmentType: "fn",
					name: "where",
					args: [[{
						segmentType: "comparison", operator: "=", args: [[{
							segmentType: "fn", name: "_splitPath",
							args: [[{
								segmentType: "literal", value: -2,
								type: { isArray: false, fhirType: "number" }
							}]],
							type: { isArray: false, fhirType: "string", schemaPath: "Reference.reference" }
						}], [{
							segmentType: "literal", value: "'" + args[0][0].value + "'",
							type: { isArray: false, fhirType: "string" }
						}]],
						type: { isArray: false, fhirType: "boolean" }
					}]],
					type: {
						isArray: true, fhirType: "Extension", schemaPath: "Extension"
					}
				}, {
					segmentType: "fn", name: "_splitPath",
					args: [[{
						segmentType: "literal", value: -1,
						type: { isArray: false, fhirType: "number" }
					}]],
					type: { isArray: false, fhirType: "string", schemaPath: "Reference.reference" }
				}];


			} else if (functionName == "getReferenceKey") {
				return [{
					segmentType: "nav", value: "reference",
					type: { isArray: false, fhirType: "string", schemaPath: "Reference.reference" }
				}, {
					segmentType: "fn", name: "_splitPath",
					args: [[{
						segmentType: "literal", value: -1,
						type: { isArray: false, fhirType: "number" }
					}]],
					type: { isArray: false, fhirType: "string", schemaPath: "Reference.reference" }
				}];
			}

			return [{ segmentType: "fn", name: functionName, args, type: { ...type, outputType } }];

		case 'LiteralTerm':
			const literalValue = node.children[0].terminalNodeText[0];
			const literalValueType = node.children[0].type.replace("Literal", "");
			if (['String', 'Number', 'Boolean', 'DateTime'].includes(literalValueType)) {
				return [{
					segmentType: "literal",
					value: literalValueType == "DateTime" ? literalValue.slice(1) : literalValue,
					type: {
						fhirType: literalValueType[0].toLowerCase() + literalValueType.slice(1),
						isArray: false
					}
				}];
			}

		case 'ExternalConstantTerm':
			return simplifyFhirPath(node.children[0], type, schema, vars);

		case 'ExternalConstant':
			const varName = node.children[0].terminalNodeText[0].replace(/`/g, '');
			const varValue = vars[varName];
			
			if (varValue === undefined) {
				throw new Error(`Variable %${varName} is not defined. Use --var ${varName}=value to define it.`);
			}
			
			const literalType = inferType(varValue);
			return [{
				segmentType: "literal",
				value: literalType === 'string' ? `'${varValue}'` : varValue,
				type: { fhirType: literalType, isArray: false }
			}];

		case 'PolarityExpression':
			return [{
				segmentType: "literal", value: node.text,
				type: { fhirType: "number", isArray: false }
			}];

		case 'ThisInvocation':
			return [{ segmentType: "this", type }]

		default:
			throw new Error(`Unhandled node type: ${node.type}`)
	}
}


function resolveType(t, s, schema) {
	let schemaPath = t.schemaPath
		? [t.schemaPath, s].join(".")
		: s;
	const elementSchema = schema[schemaPath];
	if (!elementSchema) return { schemaPath }
	if (elementSchema && elementSchema.cr) {
		schemaPath = elementSchema.cr;
	} else if (
		elementSchema.t !== "BackboneElement" &&
		elementSchema.t !== "Element" &&
		elementSchema.t[0] === elementSchema.t[0].toUpperCase()
	) {
		schemaPath = elementSchema.t
	}
	if (!elementSchema)
		throw (new Error(`Schema not found for '${schemaPath}'`));
	return {
		isArray: elementSchema.a,
		fhirType: elementSchema.t,
		schemaPath
	}
}

function inferType(value) {
	if (value === 'true' || value === 'false') return 'boolean';
	if (!isNaN(value) && value.trim() !== '') return 'number';
	if (/^\d{4}-\d{2}-\d{2}T/.test(value)) return 'dateTime';
	return 'string';
}