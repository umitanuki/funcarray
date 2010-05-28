#include "postgres.h"

#include "access/tupmacs.h"
#include "catalog/pg_proc.h"
#include "nodes/makefuncs.h"
#include "parser/parse_coerce.h"
#include "utils/array.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

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
static bool check_map_lambda_type(Oid procid, Oid element_type);
Datum maparray(PG_FUNCTION_ARGS);
Datum reducearray(PG_FUNCTION_ARGS);
Datum filterarray(PG_FUNCTION_ARGS);

/*
 *	check_map_lambda_type
 *		Check if the argument type and return type match the input type.
 *
 *	map's lambda accepts 1 argument of array's element type and
 *	returns the same type.
 *	Note that variadic functions aren't supported so far.
 */
static bool
check_map_lambda_type(Oid procid, Oid element_type)
{
	HeapTuple		ftup;
	Form_pg_proc	pform;
	oidvector	   *argtypes;
	bool			isnull;
	Oid				arg1type;

	ftup = SearchSysCache(PROCOID, ObjectIdGetDatum(procid), 0, 0, 0);
	if (!HeapTupleIsValid(ftup))
		elog(ERROR, "cache lookup failed for function %u", procid);
	pform = (Form_pg_proc) GETSTRUCT(ftup);

	/* return type is binary coercible to input type? */
	if (!can_coerce_type(1,
			&element_type, &pform->prorettype, COERCION_IMPLICIT))
	{
		ReleaseSysCache(ftup);
		return false;
	}

	/* lambda accepts 1 argument */
	if (pform->pronargs != 1)
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
	arg1type = *((Oid *) ARR_DATA_PTR(argtypes));
//	if (!IsBinaryCoercible(arg1type, element_type))
	if (!can_coerce_type(1, &element_type, &arg1type, COERCION_IMPLICIT))
	{
		ReleaseSysCache(ftup);
		return false;
	}

	ReleaseSysCache(ftup);
	return true;
}

Datum
maparray(PG_FUNCTION_ARGS)
{
	ArrayType	   *oldarray = PG_GETARG_ARRAYTYPE_P(0);
	MapContext	   *mc;
	ArrayType	   *newarray;

	if (fcinfo->flinfo->fn_extra == NULL)
	{
		/* first call */
		RegProcedure	procid = PG_GETARG_OID(1);
		MemoryContext	oldcontext;

		oldcontext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

		mc = (MapContext *) palloc(sizeof(MapContext));
		mc->element_type = ARR_ELEMTYPE(oldarray);
		get_typlenbyvalalign(ARR_ELEMTYPE(oldarray),
							&mc->typlen, &mc->typbyval, &mc->typalign);
		fmgr_info(procid, &mc->flinfo);
		mc->optimize = 0;
		if (mc->typlen == sizeof(uint32) && mc->typbyval &&
			mc->typalign == 'i')
		{
			mc->optimize = 'i';
		}
		else if (mc->typlen == sizeof(uint16) && mc->typbyval &&
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
		if (!check_map_lambda_type(procid, mc->element_type))
		{
			elog(ERROR, "function(%d) type mismatch", procid);
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		/* second or later */
		mc = (MapContext *) fcinfo->flinfo->fn_extra;
	}

	if (mc->typlen > 0)
	{
		int			i, nelems;
		size_t		bytes;
		bool		hasnull;

		bytes = ARR_SIZE(oldarray);
//		newarray = (ArrayType *) palloc(bytes);
		newarray = DatumGetArrayTypePCopy(oldarray);
		hasnull = ARR_HASNULL(oldarray);
		nelems = ArrayGetNItems(ARR_NDIM(oldarray), ARR_DIMS(oldarray));

		/*
		 *	header should look the same in new and old,
		 *	including null bitmap
		 */
//		if (hasnull)
//		{
//			memcpy(newarray, oldarray,
//				ARR_OVERHEAD_WITHNULLS(ARR_NDIM(oldarray), nelems));
//		}
//		else
//		{
//			memcpy(newarray, oldarray,
//				ARR_OVERHEAD_NONULLS(ARR_NDIM(oldarray)));
//		}

		if (mc->optimize == 'i' && !hasnull)
		{
			/* optimization for int4 without null */
			uint32	   *oldints = (uint32 *) ARR_DATA_PTR(oldarray);
			uint32	   *newints = (uint32 *) ARR_DATA_PTR(newarray);

			for(i = 0; i < nelems; i++)
			{
				newints[i] = DatumGetUInt32(
					FunctionCall1(&mc->flinfo, UInt32GetDatum(oldints[i])));
			}
		}
		else if (mc->optimize  == 's' && !hasnull)
		{
			/* optimization for int2 without null */
			uint16	   *oldints = (uint16 *) ARR_DATA_PTR(oldarray);
			uint16	   *newints = (uint16 *) ARR_DATA_PTR(newarray);

			for(i = 0; i < nelems; i++)
			{
				newints[i] = DatumGetInt16(
					FunctionCall1(&mc->flinfo, UInt16GetDatum(oldints[i])));
			}
		}
		else
		{
			/* general way */
			bits8	   *bitmap = ARR_NULLBITMAP(oldarray);
			int			bitmask = 1;
			char	   *olddata = ARR_DATA_PTR(oldarray);
			char	   *newdata = ARR_DATA_PTR(newarray);
			int			inc;

			inc = att_align_nominal(mc->typlen, mc->typalign);
			for(i = 0; i < nelems; i++)
			{
				Datum	oldelem;
				Datum	newelem;

				if (bitmap && (*bitmap & bitmask) == 0)
				{
					/* do nothing */
				}
				else
				{
					oldelem = fetch_att(olddata, mc->typbyval, mc->typlen);
					newelem = FunctionCall1(&mc->flinfo, oldelem);
					if (mc->typbyval)
						store_att_byval(newdata, newelem, mc->typlen);
					else
						memmove(newdata, DatumGetPointer(newelem), mc->typlen);

					olddata += inc;
					newdata += inc;
				}

				if (bitmap)
				{
					bitmask <<= 1;
					if (bitmask == 0x100)
					{
						bitmap++;
						bitmask = 1;
					}
				}
			}
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
			if (!nulls[i])
			{
				values[i] = FunctionCall1(&mc->flinfo, values[i]);
			}
		}

		newarray = construct_md_array(values, nulls, ARR_NDIM(oldarray),
						ARR_DIMS(oldarray), ARR_LBOUND(oldarray),
						mc->element_type, mc->typlen,
						mc->typbyval, mc->typalign);
	}

	PG_RETURN_ARRAYTYPE_P(newarray);
}

Datum
reducearray(PG_FUNCTION_ARGS)
{
	ArrayType	   *array = PG_GETARG_ARRAYTYPE_P(0);
	MapContext	   *mc;
	Datum			result = 0;

	if (fcinfo->flinfo->fn_extra == NULL)
	{
		/* first call */
		RegProcedure	procid = PG_GETARG_OID(1);
		MemoryContext	oldcontext;

		oldcontext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

		mc = (MapContext *) palloc(sizeof(MapContext));
		mc->element_type = ARR_ELEMTYPE(array);
		get_typlenbyvalalign(ARR_ELEMTYPE(array),
							&mc->typlen, &mc->typbyval, &mc->typalign);
		fmgr_info(procid, &mc->flinfo);
		mc->optimize = 0;
		if (mc->typlen == sizeof(uint32) && mc->typbyval &&
			mc->typalign == 'i')
		{
			mc->optimize = 'i';
		}
		else if (mc->typlen == sizeof(uint16) && mc->typbyval &&
			mc->typalign == 's')
		{
			mc->optimize = 's';
		}

		/* TODO: type check */
		fcinfo->flinfo->fn_extra = (void *) mc;
		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		mc = (MapContext *) fcinfo->flinfo->fn_extra;
	}

	if (mc->typlen > 0)
	{
		int			i, nelems;
		bool		hasnull;

		hasnull = ARR_HASNULL(array);
		nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

		if (nelems == 0)
		{
			PG_RETURN_NULL();
		}

		if (mc->optimize == 'i' && !hasnull)
		{
			/* optimization for int4 without null */
			uint32	   *ints = (uint32 *) ARR_DATA_PTR(array);
			uint32		state = ints[0];

			for(i = 1; i < nelems; i++)
			{
				state = DatumGetUInt32(
					FunctionCall2(&mc->flinfo,
						UInt32GetDatum(state), UInt32GetDatum(ints[i])));
			}

			result = UInt32GetDatum(state);
		}
		else if (mc->optimize == 's' && !hasnull)
		{
			/* optimization for int2 without null */
			uint16	   *ints = (uint16 *) ARR_DATA_PTR(array);
			uint16		state = ints[0];

			for(i = 1; i < nelems; i++)
			{
				state = DatumGetUInt16(
					FunctionCall2(&mc->flinfo,
						UInt16GetDatum(state), UInt16GetDatum(ints[i])));
			}

			result = UInt16GetDatum(state);
		}
		else
		{
			/* general way */
			bits8	   *bitmap = ARR_NULLBITMAP(array);
			int			bitmask = 1;
			char	   *data = ARR_DATA_PTR(array);
			bool		isnull = true;

			for(i = 0; i < nelems; i++)
			{
				Datum	elem;

				if (bitmap && (*bitmap & bitmask) == 0)
				{
					/* do nothing */
				}
				else
				{
					elem = fetch_att(data, mc->typbyval, mc->typlen);

					if (isnull)
					{
						/* fetch the first non-null input */
						result = elem;
						isnull = false;
					}
					else
					{
						result = FunctionCall2(&mc->flinfo, result, elem);
					}

					data += att_align_nominal(mc->typlen, mc->typalign);
				}

				if (bitmap)
				{
					bitmask <<= 1;
					if (bitmask == 0x100)
					{
						bitmap++;
						bitmask = 1;
					}
				}
			}

			if (isnull)
			{
				PG_RETURN_NULL();
			}
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
			if (!nulls[i])
			{
				if (isnull)
				{
					result = values[i];
					isnull = false;
				}
				else
				{
					result = FunctionCall2(&mc->flinfo, result, values[i]);
				}
			}
		}
	}

	PG_RETURN_DATUM(result);
}

Datum
filterarray(PG_FUNCTION_ARGS)
{
	ArrayType	   *oldarray = PG_GETARG_ARRAYTYPE_P(0);
	MapContext	   *mc;
	ArrayType	   *newarray;

	if (ARR_NDIM(oldarray) == 0)
		PG_RETURN_ARRAYTYPE_P(oldarray);
	else if (ARR_NDIM(oldarray) != 1)
		elog(ERROR, "cannot accept >= 2 dim");

	if (fcinfo->flinfo->fn_extra == NULL)
	{
		/* first call */
		RegProcedure	procid = PG_GETARG_OID(1);
		MemoryContext	oldcontext;

		oldcontext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

		mc = (MapContext *) palloc(sizeof(MapContext));
		mc->element_type = ARR_ELEMTYPE(oldarray);
		get_typlenbyvalalign(ARR_ELEMTYPE(oldarray),
							 &mc->typlen, &mc->typbyval, &mc->typalign);
		fmgr_info(procid, &mc->flinfo);
		mc->optimize = 0;
		if (mc->typlen == sizeof(uint32) && mc->typbyval &&
			mc->typalign == 'i')
		{
			mc->optimize = 'i';
		}
		else if (mc->typlen == sizeof(uint16) && mc->typbyval &&
			mc->typalign == 's')
		{
			mc->optimize = 's';
		}

		/* TODO: type check */
		fcinfo->flinfo->fn_extra = (void *) mc;
		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		mc = (MapContext *) fcinfo->flinfo->fn_extra;
	}

	if (mc->typlen > 0)
	{
		int			i, nelems;
		size_t		bytes;
		bool		hasnull;

		hasnull = ARR_HASNULL(oldarray);
		newarray = DatumGetArrayTypePCopy(oldarray);
		nelems = ArrayGetNItems(ARR_NDIM(oldarray), ARR_DIMS(oldarray));

		if (mc->optimize == 'i' && !hasnull)
		{
			uint32	   *oldints = (uint32 *) ARR_DATA_PTR(oldarray);
			uint32	   *newints = (uint32 *) ARR_DATA_PTR(newarray);
			int			newlen = 0;

			for(i = 0; i < nelems; i++)
			{
				if (DatumGetBool(
					FunctionCall1(&mc->flinfo, UInt32GetDatum(oldints[i]))))
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
				sizeof(uint32) * newlen);
			ARR_DIMS(newarray)[0] = newlen;
		}
		else if (mc->optimize == 's' && !hasnull)
		{
			uint16	   *oldints = (uint16 *) ARR_DATA_PTR(oldarray);
			uint16	   *newints = (uint16 *) ARR_DATA_PTR(newarray);
			int			newlen = 0;

			for(i = 0; i < nelems; i++)
			{
				if (DatumGetBool(
					FunctionCall1(&mc->flinfo, UInt16GetDatum(oldints[i]))))
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
				sizeof(uint16) * newlen);
			ARR_DIMS(newarray)[0] = newlen;
		}
		else
		{
			/* general case */
			bits8	   *bitmap = ARR_NULLBITMAP(oldarray);
			int			bitmask = 1;
			char	   *olddata = ARR_DATA_PTR(oldarray);
			char	   *newdata = ARR_DATA_PTR(newarray);
			int			inc;
			int			newlen = 0;

			inc = att_align_nominal(mc->typlen, mc->typalign);
			for(i = 0; i < nelems; i++)
			{
				Datum	oldelem;

				if (bitmap && (*bitmap & bitmask) == 0)
				{
					/* do nothing */
				}
				else
				{
					oldelem = fetch_att(olddata, mc->typbyval, mc->typlen);
					if (DatumGetBool(
						FunctionCall1(&mc->flinfo, oldelem)))
					{
						if (mc->typbyval)
							store_att_byval(newdata, oldelem, mc->typlen);
						else
							memmove(newdata, DatumGetPointer(oldelem), mc->typlen);

						newdata += inc;
						newlen++;
					}
					olddata += inc;
				}

				if (bitmap)
				{
					bitmask <<= 1;
					if (bitmask == 0x100)
					{
						bitmap++;
						bitmask = 1;
					}
				}
			}
		}
	}
	else
	{
		elog(ERROR, "variable length elements not supported so far.");
	}

	PG_RETURN_ARRAYTYPE_P(newarray);
}
