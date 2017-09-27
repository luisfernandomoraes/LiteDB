﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace LiteDB
{
    /// <summary>
    /// Compile and execute simple expressions using BsonDocuments. Used in indexes and updates operations. See https://github.com/mbdavid/LiteDB/wiki/Expressions
    /// </summary>
    public partial class BsonExpression
    {
        #region Operator Functions Cache

        private static Dictionary<string, MethodInfo> _operators = new Dictionary<string, MethodInfo>
        {
            ["/"] = typeof(BsonExpression).GetMethod("DIVIDE"),
            ["*"] = typeof(BsonExpression).GetMethod("MULTIPLY"),
            ["+"] = typeof(BsonExpression).GetMethod("ADD"),
            ["-"] = typeof(BsonExpression).GetMethod("MINUS"),
            [">"] = typeof(BsonExpression).GetMethod("GT"),
            [">="] = typeof(BsonExpression).GetMethod("GTE"),
            ["<"] = typeof(BsonExpression).GetMethod("LT"),
            ["<="] = typeof(BsonExpression).GetMethod("LTE"),
            ["="] = typeof(BsonExpression).GetMethod("EQ"),
            ["!="] = typeof(BsonExpression).GetMethod("NEQ"),
            ["&&"] = typeof(BsonExpression).GetMethod("AND"),
            ["||"] = typeof(BsonExpression).GetMethod("OR")
        };

        #endregion

        private Func<BsonDocument, BsonValue, IEnumerable<BsonValue>> _func;
        private string _expression;

        public BsonExpression(string expression)
        {
            _expression = expression;
            _func = Compile(expression, true);
        }

        /// <summary>
        /// Support full operations (be used inside array selector $.Items[expr])
        /// </summary>
        private BsonExpression(string expression, bool arithmeticOnly)
        {
            _expression = expression;
            _func = Compile(expression, arithmeticOnly);
        }

        /// <summary>
        /// Execute expression and returns IEnumerable values (can returns NULL if no elements).
        /// </summary>
        public IEnumerable<BsonValue> Execute(BsonDocument doc, bool includeNullIfEmpty = true)
        {
            return this.Execute(doc, doc, includeNullIfEmpty);
        }

        /// <summary>
        /// Execute expression and returns IEnumerable values (can returns NULL if no elements).
        /// </summary>
        public IEnumerable<BsonValue> Execute(BsonDocument root, BsonValue current, bool includeNullIfEmpty = true)
        {
            var index = 0;
            var values = _func(root, current ?? root);

            foreach (var value in values)
            {
                index++;
                yield return value;
            }

            if (index == 0 && includeNullIfEmpty) yield return BsonValue.Null;
        }

        #region Compiler

        private static Dictionary<string, Func<BsonDocument, BsonValue, IEnumerable<BsonValue>>> _cache = new Dictionary<string, Func<BsonDocument, BsonValue, IEnumerable<BsonValue>>>();

        /// <summary>
        /// Parse and compile an expression from a string. Do cache of expressions
        /// </summary>
        private static Func<BsonDocument, BsonValue, IEnumerable<BsonValue>> Compile(string expression, bool arithmeticOnly)
        {
            Func<BsonDocument, BsonValue, IEnumerable<BsonValue>> fn;

            if (_cache.TryGetValue(expression, out fn)) return fn;

            lock (_cache)
            {
                if (_cache.TryGetValue(expression, out fn)) return fn;

                var root = Expression.Parameter(typeof(BsonDocument), "root");
                var current = Expression.Parameter(typeof(BsonValue), "current");
                var expr = ParseExpression(new StringScanner(expression), root, current, arithmeticOnly);

                var lambda = Expression.Lambda<Func<BsonDocument, BsonValue, IEnumerable<BsonValue>>>(expr, root, current);

                fn = lambda.Compile();

                _cache[expression] = fn;

                return fn;
            }
        }

        #endregion

        #region Expression Parser

        private static Regex _reArithmetic = new Regex(@"^\s*(\+|\-|\*|\/)\s*");
        private static Regex _reOperator = new Regex(@"^\s*(\+|\-|\*|\/|=|!=|>=|>|<=|<|&&|\|\|)\s*");

        /// <summary>
        /// Start parse string into linq expression. Read path, function or base type bson values (int, double, bool, string)
        /// </summary>
        internal static Expression ParseExpression(StringScanner s, ParameterExpression root, ParameterExpression current, bool arithmeticOnly)
        {
            var first = ParseSingleExpression(s, root, current, false);
            var values = new List<Expression> { first };
            var ops = new List<string>();

            // read all blocks and operation first
            while (!s.HasTerminated)
            {
                // checks if must support arithmetic only (+, -, *, /)
                var op = s.Scan(arithmeticOnly ? _reArithmetic : _reOperator, 1);

                if (op.Length == 0) break;

                var expr = ParseSingleExpression(s, root, current, false);

                values.Add(expr);
                ops.Add(op);
            }

            var order = 0;

            // now, process operator in correct order
            while(values.Count >= 2)
            {
                var op = _operators.ElementAt(order);
                var n = ops.IndexOf(op.Key);

                if (n == -1)
                {
                    order++;
                }
                else
                {
                    // get left/right values to execute operator
                    var left = values.ElementAt(n);
                    var right = values.ElementAt(n + 1);

                    // process result in a single value
                    var result = Expression.Call(op.Value, left, right);

                    // remove left+right and insert result
                    values.Insert(n, result);
                    values.RemoveRange(n + 1, 2);

                    // remove operation
                    ops.RemoveAt(n);
                }
            }

            return values.Single();
        }

        /// <summary>
        /// Start parse string into linq expression. Read path, function or base type bson values (int, double, bool, string)
        /// </summary>
        internal static Expression ParseSingleExpression(StringScanner s, ParameterExpression root, ParameterExpression current, bool isRoot)
        {
            if (s.Match(@"[\$@]") || isRoot) // read root path
            {
                var r = s.Scan(@"([\$@])\.?", 1); // read root/current
                var method = typeof(BsonExpression).GetMethod("Root");
                var name = Expression.Constant(s.Scan(@"[\$\-\w]+"));
                var expr = Expression.Call(method, r == "@" ? current : root, name) as Expression;

                // parse the rest of path
                while (!s.HasTerminated)
                {
                    var result = ParsePath(s, expr, root);

                    if (result == null) break;

                    expr = result;
                }

                return expr;
            }
            else if (s.Match(@"-?\d*\.\d+")) // read double
            {
                var number = Convert.ToDouble(s.Scan(@"-?\d*\.\d+"));
                var value = Expression.Constant(new BsonValue(number));

                return Expression.NewArrayInit(typeof(BsonValue), value);
            }
            else if (s.Match(@"-?\d+")) // read int
            {
                var number = Convert.ToInt32(s.Scan(@"-?\d+"));
                var value = Expression.Constant(new BsonValue(number));

                return Expression.NewArrayInit(typeof(BsonValue), value);
            }
            else if (s.Match(@"(true|false)")) // read bool
            {
                var boolean = Convert.ToBoolean(s.Scan(@"(true|false)"));
                var value = Expression.Constant(new BsonValue(boolean));

                return Expression.NewArrayInit(typeof(BsonValue), value);
            }
            else if (s.Match(@"null")) // read null
            {
                var value = Expression.Constant(BsonValue.Null);

                return Expression.NewArrayInit(typeof(BsonValue), value);
            }
            else if (s.Match(@"'")) // read string
            {
                var str = s.Scan(@"'([\s\S]*)?'", 1);
                var value = Expression.Constant(new BsonValue(str));

                return Expression.NewArrayInit(typeof(BsonValue), value);
            }
            else if (s.Match(@"\w+\s*\(")) // read function
            {
                // get static method from this class
                var name = s.Scan(@"(\w+)\s*\(", 1).ToUpper();
                var method = typeof(BsonExpression).GetMethod(name);
                var parameters = new List<Expression>();

                if (method == null) throw LiteException.SyntaxError(s, "Method " + name + " not exist");

                while (!s.HasTerminated)
                {
                    var parameter = ParseExpression(s, root, current, false);

                    parameters.Add(parameter);

                    if (s.Scan(@"\s*,\s*").Length > 0) continue;
                    else if (s.Scan(@"\s*\)\s*").Length > 0) break;
                    throw LiteException.SyntaxError(s);
                }

                return Expression.Call(method, parameters.ToArray());
            }

            throw LiteException.SyntaxError(s);
        }

        /// <summary>
        /// Implement a JSON-Path like navigation on BsonDocument. Support a simple range of paths
        /// </summary>
        private static Expression ParsePath(StringScanner s, Expression expr, ParameterExpression root)
        {
            if (s.Match(@"\.[\$\-\w]+"))
            {
                var method = typeof(BsonExpression).GetMethod("Member");
                var name = Expression.Constant(s.Scan(@"\.([\$\-\w]+)", 1));
                return Expression.Call(method, expr, name);
            }
            else if (s.Match(@"\["))
            {
                var method = typeof(BsonExpression).GetMethod("Array");
                var i = s.Scan(@"\[\s*(-?[\d+\*])\s*\]", 1);
                var index = i != "*" && i != "" ? Convert.ToInt32(i) : int.MaxValue;
                var inner = "";

                if (i == "") // if array operation are not index based, read expression 
                {
                    s.Scan(@"\[\s*");
                    // read expression with full support to all operators/formulas
                    inner = ReadExpression(s, false, false);
                    if (inner == null) throw LiteException.SyntaxError(s, "Invalid expression formula");
                    s.Scan(@"\s*\]");
                }

                return Expression.Call(method, expr, Expression.Constant(index), Expression.Constant(inner), root);
            }
            else
            {
                return null;
            }
        }

        #endregion

        #region Expression reader from a StringScanner

        /// <summary>
        /// Extract expression or a path from a StringScanner. Returns null if is not a Path/Expression
        /// </summary>
        internal static string ReadExpression(StringScanner s, bool pathOnly, bool arithmeticOnly = true)
        {
            var start = s.Index;

            try
            {
                // if marked to read path only and first char is not $,
                // enter in parseExpressin marking as RootPath
                var isRoot = pathOnly;
                var root = Expression.Parameter(typeof(BsonDocument), "root");
                var current = Expression.Parameter(typeof(BsonValue), "current");

                if (pathOnly)
                {
                    // if read path, read first expression only
                    // support missing $ as root
                    s.Scan(@"\$\.?");
                    ParseSingleExpression(s, root, current, true);
                }
                else
                {
                    // read all expression (a + b)
                    // if include operator, support = > < && || too
                    ParseExpression(s, root, current, arithmeticOnly);
                }

                return s.Source.Substring(start, s.Index - start);
            }
            catch (LiteException ex) when (ex.ErrorCode == LiteException.SYNTAX_ERROR)
            {
                s.Index = start;
                return null;
            }
        }

        #endregion

        #region Path static methods

        /// <summary>
        /// Returns value from root document. Returns same document if name are empty
        /// </summary>
        public static IEnumerable<BsonValue> Root(BsonValue value, string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                yield return value;
            }
            else if (value.IsDocument)
            {
                if (value.AsDocument.TryGetValue(name, out BsonValue item))
                {
                    yield return item;
                }
            }
        }

        /// <summary>
        /// Return a value from a value as document. If has no name, just return values ($). If value are not a document, do not return anything
        /// </summary>
        public static IEnumerable<BsonValue> Member(IEnumerable<BsonValue> values, string name)
        {
            foreach (var doc in values.Where(x => x.IsDocument).Select(x => x.AsDocument))
            {
                if (doc.TryGetValue(name, out BsonValue item))
                {
                    yield return item;
                }
            }
        }

        /// <summary>
        /// Returns all values from array according index. If index are MaxValue, return all values
        /// </summary>
        public static IEnumerable<BsonValue> Array(IEnumerable<BsonValue> values, int index, string expr, BsonDocument root)
        {
            foreach (var value in values)
            {
                if (value.IsArray)
                {
                    var arr = value.AsArray;

                    // [expression(Func<BsonValue, bool>)]
                    if (expr != "")
                    {
                        var e = new BsonExpression(expr, false);

                        foreach (var item in arr)
                        {
                            // execute for each child value and except a first bool value (returns if true)
                            var c = e.Execute(root, item, true).First();

                            if (c.IsBoolean && c.AsBoolean == true)
                            {
                                yield return item;
                            }
                        }
                    }
                    // [all]
                    else if (index == int.MaxValue)
                    {
                        foreach (var item in arr)
                        {
                            yield return item;
                        }
                    }
                    // [fixed_index]
                    else
                    {
                        var idx = index < 0 ? arr.Count + index : index;

                        if (arr.Count > idx)
                        {
                            yield return arr[idx];
                        }
                    }
                }
            }
        }

        #endregion

        public override string ToString()
        {
            return _expression;
        }
    }
}