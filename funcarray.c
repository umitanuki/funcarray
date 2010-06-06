#include "postgres.h"

#include "access/tupmacs.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "parser/parse_coerce.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(maparray);
PG_FUNCTION_INFO_V1(reducearray);
PG_FUNCTION_INFO_V1(filterarray);

/*
 * A small version of ArrayMetaState + FmgrInfo
 */
typedef struct
{
	Oid			element_type;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	FmgrInfo	flinfo;
	char		optimize;	/* 'i', 's' */
} MapContext;

/* prototype */
static bool check_lambda_type(Oid procid, Oid element_type,
			Oid returned_type, int nargs);
Datum maparray(PG_FUNCTION_ARGS);
Datum reducearray(PG_FUNCTION_ARGS);
Datum filterarray(PG_FUNCTION_ARGS);


/*
 *	check_lambda_type
 *		Check if the argument type and return type match the input type.
 *
 *	lambda function accepts required arguments of array's element type and
 *	returns the specified type. Polymorphoic types are allowed.
 *	Note that variadic functions aren't supported so far.
 */
static bool
check_lambda_type(Oid procid, Oid element_type, Oid returned_type, int nargs)
{
	HeapTuple		ftup;
	Form_pg_proc	pform;
	oidvector	   *argtypes;
	bool			isnull;
	int				i;
	Oid			   *types;
	Oid				coerce_funcid;

	ftup = SearchSysCache(PROCOID, ObjectIdGetDatum(procid), 0, 0, 0);
	if (!HeapTupleIsValid(ftup))
		elog(ERROR, "cache lookup failed for function %u", procid);
	pform = (Form_pg_proc) GETSTRUCT(ftup);

	/* return type is binary coercible to input type? */
	if (!can_coerce_type(1,
			&pform->prorettype, &returned_type, COERCION_IMPLICIT))
	{
		ReleaseSysCache(ftup);
		return false;
	}

	/* lambda accepts exactly required number of arguments */
	if (pform->pronargs != nargs)
	{
		ReleaseSysCache(ftup);
		return false;
	}

	argtypes = (oidvector *) SysCacheGetAttr(PROCOID,
						ftup, Anum_pg_proc_proargtypes, &isnull);
	if (isnull)
	{
		ReleaseSysCache(ftup);
		return false;
	}

	/* argument type must match the input type */
	types = (Oid *) ARR_DATA_PTR(argtypes);
	for(i = 0; i < nargs; i++)
	{
		Oid		argtype = types[i];

		if (!can_coerce_type(1, &element_type, &argtype, COERCION_IMPLICIT))
		{
			ReleaseSysCache(ftup);
			return false;
		}

		/*
		 * can_coerce_type() doesn't report COERCION_PATH_FUNC case.
		 * We need binary coercible case only.
		 */
		if (find_coercion_pathway(argtype,
				element_type, COERCION_IMPLICIT, &coerce_funcid) !=
				COERCION_PATH_RELABELTYPE)
		{
			ReleaseSysCache(ftup);
			return false;
		}
	}

	ReleaseSysCache(ftup);
	return true;
}

Datum
maparray(PG_FUNCTION_ARGS)
{
	ArrayType	   *oldarray = PG_GETARG_ARRAYTYPE_P(0);
	RegProcedure	procid = PG_GETARG_OID(1);
	MapContext	   *mc;
	ArrayType	   *newarray;

	if (fcinfo->flinfo->fn_extra == NULL ||
		((MapContext *) fcinfo->flinfo->fn_extra)->flinfo.fn_oid != procid)
	{
		/* set up context */
		MemoryContext	oldcontext;

		if (fcinfo->flinfo->fn_extra)
			pfree(fcinfo->flinfo->fn_extra);

		oldcontext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

		mc = (MapContext *) palloc(sizeof(MapContext));
		mc->element_type = ARR_ELEMTYPE(oldarray);
		get_typlenbyvalalign(ARR_ELEMTYPE(oldarray),
							&mc->typlen, &mc->typbyval, &mc->typalign);
		fmgr_info(procid, &mc->flinfo);
		mc->optimize = 0;
		if (mc->typlen == sizeof(int32) && mc->typbyval &&
			mc->typalign == 'i')
		{
			mc->optimize = 'i';
		}
		else if (mc->typlen == sizeof(int64) && mc->typbyval &&
			mc->typalign == 'd')
		{
			mc->optimize = 'd';
		}
		else if (mc->typlen == sizeof(int16) && mc->typbyval &&
			mc->typalign == 's')
		{
			mc->optimize = 's';
		}

		{
			/* param is dummy for polymorphic argument */
			Param	   *param = makeNode(Param);
			param->paramkind = PARAM_EXTERN;
			param->paramtype = mc->element_type;
			param->paramtypmod = -1; /* function input has always -1 */
			param->location = -1;
			mc->flinfo.fn_expr = (Node *) makeFuncExpr(
									procid,
									mc->element_type,
									list_make1(param),
									COERCE_EXPLICIT_CAST);
		}

		fcinfo->flinfo->fn_extra = (void *) mc;

		/* type check */
		if (!check_lambda_type(procid, mc->element_type, mc->element_type, 1))
		{
			elog(ERROR, "function %s type mismatch",
				format_procedure(procid));
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		/* restore the previous call info */
		mc = (MapContext *) fcinfo->flinfo->fn_extra;
	}

	if (mc->optimize > 0 && !ARR_HASNULL(oldarray))
	{
		int			i, nelems;
		size_t		bytes;
		FunctionCallInfoData	myinfo;

		bytes = ARR_SIZE(oldarray);
		newarray = DatumGetArrayTypePCopy(oldarray);
		nelems = ArrayGetNItems(ARR_NDIM(oldarray), ARR_DIMS(oldarray));
		InitFunctionCallInfoData(myinfo, &mc->flinfo, 1, NULL, NULL);

		if (mc->optimize == 'i')
		{
			/* optimization for int4 without null */
			int32	   *oldints = (int32 *) ARR_DATA_PTR(oldarray);
			int32	   *newints = (int32 *) ARR_DATA_PTR(newarray);

			for(i = 0; i < nelems; i++)
			{
				myinfo.arg[0] = Int32GetDatum(oldints[i]);
				newints[i] = DatumGetInt32(FunctionCallInvoke(&myinfo));
				if (myinfo.isnull)
				{
					elog(ERROR, "function %s returned NULL",
						format_procedure(mc->flinfo.fn_oid));
				}
			}
		}
		else if (mc->optimize == 'd')
		{
			/*
			 * optimization for int8 wtihout null
			 * Note that this path is processed only when int8 is byval.
			 */
			int64	   *oldints = (int64 *) ARR_DATA_PTR(oldarray);
			int64	   *newints = (int64 *) ARR_DATA_PTR(newarray);

			for(i = 0; i < nelems; i++)
			{
				myinfo.arg[0] = Int64GetDatum(oldints[i]);
				newints[i] = DatumGetInt64(FunctionCallInvoke(&myinfo));
				if (myinfo.isnull)
				{
					elog(ERROR, "function %s returned NULL",
						format_procedure(mc->flinfo.fn_oid));
				}
			}
		}
		else if (mc->optimize == 's')
		{
			/* optimization for int2 without null */
			int16	   *oldints = (int16 *) ARR_DATA_PTR(oldarray);
			int16	   *newints = (int16 *) ARR_DATA_PTR(newarray);

			for(i = 0; i < nelems; i++)
			{
				myinfo.arg[0] = Int16GetDatum(oldints[i]);
				newints[i] = DatumGetInt16(FunctionCallInvoke(&myinfo));
				if (myinfo.isnull)
				{
					elog(ERROR, "function %s returned NULL",
						format_procedure(mc->flinfo.fn_oid));
				}
			}
		}
		else
		{
			elog(ERROR, "unknown optimization code = %c", mc->optimize);
		}
	}
	else
	{
		/*
		 *	variable length element types:
		 *	speed is secondary here, but flexibility is primary.
		 */
		Datum	   *values;
		bool	   *nulls;
		int			i, nelems;

		deconstruct_array(oldarray, mc->element_type,
						  mc->typlen, mc->typbyval, mc->typalign,
						  &values, &nulls, &nelems);

		for(i = 0; i < nelems; i++)
		{
			if (!nulls[i] || !mc->flinfo.fn_strict)
			{
				FunctionCallInfoData	myinfo;

				InitFunctionCallInfoData(myinfo, &mc->flinfo, 1, NULL, NULL);
				myinfo.arg[0] = values[i];
				myinfo.argnull[0] = nulls[i];

				values[i] = FunctionCallInvoke(&myinfo);
				nulls[i] = myinfo.isnull;
			}
		}

		newarray = construct_md_array(values, nulls, ARR_NDIM(oldarray),
						ARR_DIMS(oldarray), ARR_LBOUND(oldarray),
						mc->element_type, mc->typlen,
						mc->typbyval, mc->typalign);
		pfree(values);
		pfree(nulls);
	}

	PG_RETURN_ARRAYTYPE_P(newarray);
}

Datum
reducearray(PG_FUNCTION_ARGS)
{
	ArrayType	   *array = PG_GETARG_ARRAYTYPE_P(0);
	RegProcedure	procid = PG_GETARG_OID(1);
	MapContext	   *mc;
	Datum			result = 0;

	if (fcinfo->flinfo->fn_extra == NULL ||
		((MapContext *) fcinfo->flinfo->fn_extra)->flinfo.fn_oid != procid)
	{
		/* set up context */
		MemoryContext	oldcontext;

		if (fcinfo->flinfo->fn_extra)
			pfree(fcinfo->flinfo->fn_extra);

		oldcontext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

		mc = (MapContext *) palloc(sizeof(MapContext));
		mc->element_type = ARR_ELEMTYPE(array);
		get_typlenbyvalalign(ARR_ELEMTYPE(array),
							&mc->typlen, &mc->typbyval, &mc->typalign);
		fmgr_info(procid, &mc->flinfo);
		mc->optimize = 0;
		if (mc->typlen == sizeof(int32) && mc->typbyval &&
			mc->typalign == 'i')
		{
			mc->optimize = 'i';
		}
		else if (mc->typlen == sizeof(int64) && mc->typbyval &&
			mc->typalign == 'd')
		{
			mc->optimize = 'd';
		}
		else if (mc->typlen == sizeof(int16) && mc->typbyval &&
			mc->typalign == 's')
		{
			mc->optimize = 's';
		}

		{
			/* param is dummy for polymorphic argument */
			Param	   *param = makeNode(Param);
			param->paramkind = PARAM_EXTERN;
			param->paramtype = mc->element_type;
			param->paramtypmod = -1; /* function input has always -1 */
			param->location = -1;
			mc->flinfo.fn_expr = (Node *) makeFuncExpr(
									procid,
									mc->element_type,
									list_make2(param, copyObject(param)),
									COERCE_EXPLICIT_CAST);
		}

		fcinfo->flinfo->fn_extra = (void *) mc;

		/* type check */
		if (!check_lambda_type(procid, mc->element_type, mc->element_type, 2))
		{
			elog(ERROR, "function %s type mismatch",
				format_procedure(procid));
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		mc = (MapContext *) fcinfo->flinfo->fn_extra;
	}

	if (mc->optimize > 0 && !ARR_HASNULL(array))
	{
		int			i, nelems;
		FunctionCallInfoData	myinfo;

		nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
		InitFunctionCallInfoData(myinfo, &mc->flinfo, 2, NULL, NULL);

		if (nelems == 0)
		{
			PG_RETURN_NULL();
		}

		if (mc->optimize == 'i')
		{
			/* optimization for int4 without null */
			int32	   *ints = (int32 *) ARR_DATA_PTR(array);

			myinfo.arg[0] = Int32GetDatum(ints[0]);
			myinfo.argnull[0] = false;
			myinfo.argnull[1] = false;

			for(i = 1; i < nelems; i++)
			{
				myinfo.arg[1] = Int32GetDatum(ints[i]);
				myinfo.arg[0] = FunctionCallInvoke(&myinfo);
				if (myinfo.isnull)
				{
					elog(ERROR, "function %s returned NULL",
						format_procedure(mc->flinfo.fn_oid));
				}
			}

			result = Int32GetDatum(myinfo.arg[0]);
		}
		else if (mc->optimize == 'd')
		{
			/*
			 * optimization for int8 without null.
			 * Note that this path is processed only when int8 is byval.
			 */
			int64	   *ints = (int64 *) ARR_DATA_PTR(array);

			myinfo.arg[0] = Int64GetDatum(ints[0]);
			myinfo.argnull[0] = false;
			myinfo.argnull[1] = false;

			for(i = 1; i < nelems; i++)
			{
				myinfo.arg[1] = Int64GetDatum(ints[i]);
				myinfo.arg[0] = FunctionCallInvoke(&myinfo);
				if (myinfo.isnull)
				{
					elog(ERROR, "function %s returned NULL",
						format_procedure(mc->flinfo.fn_oid));
				}
			}

			result = Int64GetDatum(myinfo.arg[0]);
		}
		else if (mc->optimize == 's')
		{
			/* optimization for int2 without null */
			uint16	   *ints = (uint16 *) ARR_DATA_PTR(array);

			myinfo.arg[0] = Int16GetDatum(ints[0]);
			myinfo.argnull[0] = false;
			myinfo.argnull[1] = false;

			for(i = 1; i < nelems; i++)
			{
				myinfo.arg[1] = Int16GetDatum(ints[i]);
				myinfo.arg[0] = FunctionCallInvoke(&myinfo);
				if (myinfo.isnull)
				{
					elog(ERROR, "function %s returned NULL",
						format_procedure(mc->flinfo.fn_oid));
				}
			}

			result = Int16GetDatum(myinfo.arg[0]);
		}
		else
		{
			elog(ERROR, "unknown optimization code %c", mc->optimize);
		}
	}
	else
	{
		/*
		 *	variable length element types:
		 *	speed is secondary here, but flexibility is primary.
		 */
		Datum	   *values;
		bool	   *nulls;
		int			i, nelems;
		bool		isnull = true;

		deconstruct_array(array, mc->element_type,
						  mc->typlen, mc->typbyval, mc->typalign,
						  &values, &nulls, &nelems);

		if (nelems == 0)
		{
			PG_RETURN_NULL();
		}

		for(i = 0; i < nelems; i++)
		{
			
			if (nulls[i] && mc->flinfo.fn_strict)
			{
				continue;
			}
			if (isnull)
			{
				result = values[i];
				isnull = false;
			}
			else
			{
				FunctionCallInfoData	myinfo;

				InitFunctionCallInfoData(myinfo, &mc->flinfo, 2, NULL, NULL);
				myinfo.arg[0] = result;
				myinfo.argnull[0] = false;
				myinfo.arg[1] = values[i];
				myinfo.argnull[1] = nulls[i];

				result = FunctionCallInvoke(&myinfo);
			}
		}
		pfree(values);
		pfree(nulls);

		if (isnull)
		{
			PG_RETURN_NULL();
		}
	}

	PG_RETURN_DATUM(result);
}

Datum
filterarray(PG_FUNCTION_ARGS)
{
	ArrayType	   *oldarray = PG_GETARG_ARRAYTYPE_P(0);
	RegProcedure	procid = PG_GETARG_OID(1);
	MapContext	   *mc;
	ArrayType	   *newarray;

	if (ARR_NDIM(oldarray) == 0)
		PG_RETURN_ARRAYTYPE_P(oldarray);
	else if (ARR_NDIM(oldarray) != 1)
		elog(ERROR, "cannot accept >= 2 dim");

	if (fcinfo->flinfo->fn_extra == NULL ||
		((MapContext *) fcinfo->flinfo->fn_extra)->flinfo.fn_oid != procid)
	{
		/* set up context */
		MemoryContext	oldcontext;

		oldcontext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

		mc = (MapContext *) palloc(sizeof(MapContext));
		mc->element_type = ARR_ELEMTYPE(oldarray);
		get_typlenbyvalalign(ARR_ELEMTYPE(oldarray),
							 &mc->typlen, &mc->typbyval, &mc->typalign);
		fmgr_info(procid, &mc->flinfo);
		mc->optimize = 0;
		if (mc->typlen == sizeof(int32) && mc->typbyval &&
			mc->typalign == 'i')
		{
			mc->optimize = 'i';
		}
		else if (mc->typlen == sizeof(int64) && mc->typbyval &&
			mc->typalign == 'd')
		{
			mc->optimize = 'd';
		}
		else if (mc->typlen == sizeof(int16) && mc->typbyval &&
			mc->typalign == 's')
		{
			mc->optimize = 's';
		}

		{
			/* param is dummy for polymorphic argument */
			Param	   *param = makeNode(Param);
			param->paramkind = PARAM_EXTERN;
			param->paramtype = mc->element_type;
			param->paramtypmod = -1; /* function input has always -1 */
			param->location = -1;
			mc->flinfo.fn_expr = (Node *) makeFuncExpr(
									procid,
									mc->element_type,
									list_make1(param),
									COERCE_EXPLICIT_CAST);
		}

		fcinfo->flinfo->fn_extra = (void *) mc;

		if (!check_lambda_type(procid, mc->element_type, BOOLOID, 1))
		{
			elog(ERROR, "function %s type mismatch",
				format_procedure(procid));
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		mc = (MapContext *) fcinfo->flinfo->fn_extra;
	}

	if (mc->optimize > 0 && !ARR_HASNULL(oldarray))
	{
		int			i, nelems;
		FunctionCallInfoData	myinfo;

		newarray = DatumGetArrayTypePCopy(oldarray);
		nelems = ArrayGetNItems(ARR_NDIM(oldarray), ARR_DIMS(oldarray));
		InitFunctionCallInfoData(myinfo, &mc->flinfo, 1, NULL, NULL);
		myinfo.argnull[0] = false;

		if (mc->optimize == 'i')
		{
			int32	   *oldints = (int32 *) ARR_DATA_PTR(oldarray);
			int32	   *newints = (int32 *) ARR_DATA_PTR(newarray);
			int			newlen = 0;

			for(i = 0; i < nelems; i++)
			{
				myinfo.arg[0] = Int32GetDatum(oldints[i]);
				/*
				 *	isnull = false everytime as it may be set true
				 *	in the previous call
				 */
				myinfo.isnull = false;
				if (DatumGetBool(FunctionCallInvoke(&myinfo)) &&
					!myinfo.isnull)
				{
					newints[newlen++] = oldints[i];
				}
			}

			if (newlen == 0)
			{
				PG_RETURN_NULL();
			}

			SET_VARSIZE(newarray,
				ARR_OVERHEAD_NONULLS(ARR_NDIM(newarray)) +
				sizeof(int32) * newlen);
			ARR_DIMS(newarray)[0] = newlen;
		}
		else if (mc->optimize == 'd')
		{
			int64	   *oldints = (int64 *) ARR_DATA_PTR(oldarray);
			int64	   *newints = (int64 *) ARR_DATA_PTR(newarray);
			int			newlen = 0;

			for(i = 0; i < nelems; i++)
			{
				myinfo.arg[0] = Int64GetDatum(oldints[i]);
				myinfo.isnull = false;
				if (DatumGetBool(FunctionCallInvoke(&myinfo)) &&
					!myinfo.isnull)
				{
					newints[newlen++] = oldints[i];
				}
			}

			if (newlen == 0)
			{
				PG_RETURN_NULL();
			}

			SET_VARSIZE(newarray,
				ARR_OVERHEAD_NONULLS(ARR_NDIM(newarray)) +
				sizeof(int64) * newlen);
			ARR_DIMS(newarray)[0] = newlen;
		}
		else if (mc->optimize == 's')
		{
			int16	   *oldints = (int16 *) ARR_DATA_PTR(oldarray);
			int16	   *newints = (int16 *) ARR_DATA_PTR(newarray);
			int			newlen = 0;

			for(i = 0; i < nelems; i++)
			{
				myinfo.arg[0] = Int16GetDatum(oldints[i]);
				myinfo.isnull = false;
				if (DatumGetBool(FunctionCallInvoke(&myinfo)) &&
					!myinfo.isnull)
				{
					newints[newlen++] = oldints[i];
				}
			}

			if (newlen == 0)
			{
				PG_RETURN_NULL();
			}

			SET_VARSIZE(newarray,
				ARR_OVERHEAD_NONULLS(ARR_NDIM(newarray)) +
				sizeof(int16) * newlen);
			ARR_DIMS(newarray)[0] = newlen;
		}
		else
		{
			elog(ERROR, "unknown optimization code = %c", mc->optimize);
		}
	}
	else
	{
		Datum	   *values;
		bool	   *nulls;
		int			i, nelems;
		int			newlen;

		deconstruct_array(oldarray, mc->element_type,
						  mc->typlen, mc->typbyval, mc->typalign,
						  &values, &nulls, &nelems);

		if (nelems == 0)
		{
			PG_RETURN_NULL();
		}

		newlen = 0;
		/*
		 *	To avoid allocate double datum array,
		 *	we overwrite the new value on the old array.
		 *	This is possible since filter() processes values
		 *	from head one by one.
		 */
		for(i = 0; i < nelems; i++)
		{
			if (!nulls[i] || !mc->flinfo.fn_strict)
			{
				FunctionCallInfoData	myinfo;

				InitFunctionCallInfoData(myinfo, &mc->flinfo, 1, NULL, NULL);
				myinfo.arg[0] = values[i];
				myinfo.argnull[0] = nulls[i];
				if (DatumGetBool(FunctionCallInvoke(&myinfo)) &&
					!myinfo.isnull)
				{
					values[newlen] = values[i];
					nulls[newlen] = nulls[i];
					newlen++;
				}
			}
			/*
			 *	null input on strict function means NULL result,
			 *	which means the value is 'false', and filter it.
			 */
		}

		newarray = construct_md_array(values, nulls, ARR_NDIM(oldarray),
							&newlen, ARR_LBOUND(oldarray),
							mc->element_type, mc->typlen,
							mc->typbyval, mc->typalign);
		pfree(values);
		pfree(nulls);
	}

	PG_RETURN_ARRAYTYPE_P(newarray);
}
