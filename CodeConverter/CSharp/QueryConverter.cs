﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ICSharpCode.CodeConverter.Shared;
using ICSharpCode.CodeConverter.Util;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Operations;
using Microsoft.CodeAnalysis.VisualBasic.Syntax;
using CSSyntax = Microsoft.CodeAnalysis.CSharp.Syntax;
using VBSyntax = Microsoft.CodeAnalysis.VisualBasic.Syntax;

namespace ICSharpCode.CodeConverter.CSharp
{
    /// <remarks>
    ///  Grammar info: https://web.archive.org/web/20170715190715/http://kursinfo.himolde.no/in-kurs/IBE150/VBspec.htm#_Toc248253288
    /// </remarks>
    internal class QueryConverter
    {
        private readonly CommentConvertingVisitorWrapper _triviaConvertingVisitor;
        private readonly SemanticModel _semanticModel;

        public QueryConverter(CommonConversions commonConversions, SemanticModel semanticModel, CommentConvertingVisitorWrapper triviaConvertingExpressionVisitor)
        {
            CommonConversions = commonConversions;
            _semanticModel = semanticModel;
            _triviaConvertingVisitor = triviaConvertingExpressionVisitor;
        }

        private CommonConversions CommonConversions { get; }

        public async Task<CSharpSyntaxNode> ConvertClausesAsync(SyntaxList<VBSyntax.QueryClauseSyntax> clauses)
        {
            var vbBodyClauses = new Queue<VBSyntax.QueryClauseSyntax>(clauses);
            var vbStartClause = vbBodyClauses.Dequeue();
            var agg = vbStartClause as VBSyntax.AggregateClauseSyntax;
            if (agg != null) {
                foreach (var queryOperators in agg.AdditionalQueryOperators) {
                    vbBodyClauses.Enqueue(queryOperators);
                }
            }
            var fromClauseSyntax = vbStartClause is VBSyntax.FromClauseSyntax fcs ? await ConvertFromClauseSyntaxAsync(fcs) : await ConvertAggregateToFromClauseSyntaxAsync((VBSyntax.AggregateClauseSyntax) vbStartClause);
            CSharpSyntaxNode rootExpression;
            if (vbBodyClauses.Any()) {
                var querySegments = await GetQuerySegmentsAsync(vbBodyClauses);
                rootExpression = await ConvertQuerySegmentsAsync(querySegments, fromClauseSyntax);
            } else {
                rootExpression = fromClauseSyntax.Expression;
            }

            if (agg != null) {
                if (agg.AggregationVariables.Count == 1 &&
                    agg.AggregationVariables.Single().Aggregation is VBSyntax.FunctionAggregationSyntax fas) {
                    if (rootExpression is CSSyntax.QueryExpressionSyntax qes)
                        rootExpression = SyntaxFactory.ParenthesizedExpression(qes);
                    var collectionRangeVariableSyntax = agg.Variables.Single();
                    var toAggregate = await fas.Argument.AcceptAsync<CSharpSyntaxNode>(_triviaConvertingVisitor);
                    var methodTocall =
                        SyntaxFactory.IdentifierName(CommonConversions.ConvertIdentifier(fas.FunctionName)); //TODO
                    var rootWithMethodCall =
                        SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression,
                            (CSSyntax.ExpressionSyntax)rootExpression, methodTocall);
                    var parameterSyntax = SyntaxFactory.Parameter(
                        CommonConversions.ConvertIdentifier(collectionRangeVariableSyntax.Identifier.Identifier));
                    var argumentSyntaxes = toAggregate != null
                        ? new[] {
                            SyntaxFactory.Argument(SyntaxFactory.SimpleLambdaExpression(
                                parameterSyntax, toAggregate))
                        }
                        : Array.Empty<CSSyntax.ArgumentSyntax>();
                    var args = SyntaxFactory.ArgumentList(SyntaxFactory.SeparatedList(argumentSyntaxes));
                    var variable = SyntaxFactory.InvocationExpression(rootWithMethodCall, args);
                    return variable;
                } else {
                    throw new NotImplementedException("Aggregate clause type not implemented");
                }
            }

            return rootExpression;
        }

        /// <summary>
        ///  TODO: Don't bother with reversing, rewrite ConvertQueryWithContinuation to recurse on them the right way around
        /// </summary>
        private async Task<List<(Queue<(SyntaxList<CSSyntax.QueryClauseSyntax>, VBSyntax.QueryClauseSyntax)>, VBSyntax.QueryClauseSyntax)>> GetQuerySegmentsAsync(Queue<VBSyntax.QueryClauseSyntax> vbBodyClauses)
        {
            var querySegments =
                new List<(Queue<(SyntaxList<CSSyntax.QueryClauseSyntax>, VBSyntax.QueryClauseSyntax)>,
                    VBSyntax.QueryClauseSyntax)>();
            while (vbBodyClauses.Any()) {
                var querySectionsReversed =
                    new Queue<(SyntaxList<CSSyntax.QueryClauseSyntax>, VBSyntax.QueryClauseSyntax)>();
                while (vbBodyClauses.Any() && !RequiresMethodInvocation(vbBodyClauses.Peek()) && !EndsInSelect(querySectionsReversed)) {
                    var convertedClauses = new List<CSSyntax.QueryClauseSyntax>();
                    while (IsPartOfSegment(vbBodyClauses)) {
                        convertedClauses.Add(await ConvertQueryBodyClauseAsync(vbBodyClauses.Dequeue()));
                    }

                    var convertQueryBodyClauses = (SyntaxFactory.List(convertedClauses),
                        vbBodyClauses.Any() && !RequiresMethodInvocation(vbBodyClauses.Peek()) ? vbBodyClauses.Dequeue() : null);
                    querySectionsReversed.Enqueue(convertQueryBodyClauses);
                }
                querySegments.Add((querySectionsReversed, vbBodyClauses.Any() && !EndsInSelect(querySectionsReversed) ? vbBodyClauses.Dequeue() : null));
            }
            return querySegments;
        }

        private static bool EndsInSelect(Queue<(SyntaxList<CSSyntax.QueryClauseSyntax>, QueryClauseSyntax)> querySectionsReversed) =>
            querySectionsReversed.LastOrDefault().Item2 is VBSyntax.SelectClauseSyntax;

        private static bool IsPartOfSegment(Queue<QueryClauseSyntax> vbBodyClauses) =>
            vbBodyClauses.Any() && !RequiredContinuation(vbBodyClauses) && !RequiresMethodInvocation(vbBodyClauses.Peek());

        private static bool RequiredContinuation(Queue<QueryClauseSyntax> vbBodyClauses) =>
            RequiredContinuation(vbBodyClauses.Peek(), vbBodyClauses.Count - 1);

        private async Task<CSharpSyntaxNode> ConvertQuerySegmentsAsync(IEnumerable<(Queue<(SyntaxList<CSSyntax.QueryClauseSyntax>, VBSyntax.QueryClauseSyntax)>, VBSyntax.QueryClauseSyntax)> querySegments, CSSyntax.FromClauseSyntax fromClauseSyntax)
        {
            CSSyntax.ExpressionSyntax query = null;
            foreach (var (queryContinuation, queryEnd) in querySegments) {
                query = (CSSyntax.ExpressionSyntax)await ConvertQueryWithContinuationsAsync(queryContinuation, fromClauseSyntax);
                var reusableFromCsId = fromClauseSyntax.Identifier.WithoutSourceMapping();
                if (queryEnd is not null) {
                    query = await ConvertQueryToLinqAsync(reusableFromCsId, queryEnd, query);
                }
                fromClauseSyntax = SyntaxFactory.FromClause(reusableFromCsId, query);
            }

            return query ?? throw new ArgumentOutOfRangeException(nameof(querySegments), querySegments, null);
        }

        private async Task<CSSyntax.InvocationExpressionSyntax> ConvertQueryToLinqAsync(SyntaxToken reusableCsFromId, VBSyntax.QueryClauseSyntax queryEnd,
            CSSyntax.ExpressionSyntax query)
        {
            var linqMethodName = GetLinqMethodName(queryEnd);
            var parenthesizedQuery = query is CSSyntax.QueryExpressionSyntax ? SyntaxFactory.ParenthesizedExpression(query) : query;
            var linqMethod = SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression, parenthesizedQuery,
                SyntaxFactory.IdentifierName(linqMethodName));
            var linqArguments = await GetLinqArgumentsAsync(reusableCsFromId, queryEnd);
            var linqArgumentList = SyntaxFactory.ArgumentList(
                SyntaxFactory.SeparatedList(linqArguments.Select(SyntaxFactory.Argument)));
            var invocationExpressionSyntax = SyntaxFactory.InvocationExpression(linqMethod, linqArgumentList);
            return invocationExpressionSyntax;
        }

        private async Task<CSharpSyntaxNode> ConvertQueryWithContinuationsAsync(Queue<(SyntaxList<CSSyntax.QueryClauseSyntax>, VBSyntax.QueryClauseSyntax)> queryContinuation, CSSyntax.FromClauseSyntax fromClauseSyntax)
        {
            var subQuery = await ConvertQueryWithContinuationAsync(queryContinuation, fromClauseSyntax.Identifier.WithoutSourceMapping());
            return subQuery != null ? SyntaxFactory.QueryExpression(fromClauseSyntax, subQuery) : fromClauseSyntax.Expression;
        }

        private async Task<CSSyntax.QueryBodySyntax> ConvertQueryWithContinuationAsync(Queue<(SyntaxList<CSSyntax.QueryClauseSyntax>, VBSyntax.QueryClauseSyntax)> querySectionsReversed, SyntaxToken reusableCsFromId)
        {
            if (!querySectionsReversed.Any()) return null;
            var (convertedClauses, clauseEnd) = querySectionsReversed.Dequeue();

            var nestedClause = await ConvertQueryWithContinuationAsync(querySectionsReversed, reusableCsFromId);
            return await ConvertSubQueryAsync(reusableCsFromId, clauseEnd, nestedClause, convertedClauses);
        }

        private async Task<CSSyntax.QueryBodySyntax> ConvertSubQueryAsync(SyntaxToken reusableCsFromId, VBSyntax.QueryClauseSyntax clauseEnd,
            CSSyntax.QueryBodySyntax nestedClause, SyntaxList<CSSyntax.QueryClauseSyntax> convertedClauses)
        {
            CSSyntax.SelectOrGroupClauseSyntax selectOrGroup;
            CSSyntax.QueryContinuationSyntax queryContinuation = null;
            switch (clauseEnd) {
                case null:
                    selectOrGroup = CreateDefaultSelectClause(reusableCsFromId);
                    break;

                case VBSyntax.GroupByClauseSyntax gcs:
                    var continuationClauses = SyntaxFactory.List<CSSyntax.QueryClauseSyntax>();
                    var isContinuation = nestedClause != null;
                    var groupKeyIds = GetGroupKeyIdentifiers(gcs).ToList();
                    var vbGroupAlias = GetVbGroupAlias(gcs);

                    var groupIdentifier = GetGroupIdentifier(isContinuation, vbGroupAlias);
                    var groupExpression = SyntaxFactory.IdentifierName(groupIdentifier);

                    // let clauses group keys
                    continuationClauses = AddGroupKeyLetClauses(groupExpression, groupKeyIds, continuationClauses);

                    // let clause group variable
                    if (isContinuation)
                    {
                        continuationClauses = AddGroupLetClause(vbGroupAlias, groupExpression, continuationClauses);
                    }

                    // let clauses aggregate functions
                    continuationClauses = AddAggregateFunctionLetClauses(gcs, groupExpression, continuationClauses);

                    queryContinuation = isContinuation
                        ? CreateGroupByContinuation(continuationClauses, nestedClause, groupIdentifier)
                        : null;

                    var identifierNameSyntax = !gcs.Items.Any()
                        ? SyntaxFactory.IdentifierName(reusableCsFromId)
                        : await gcs.Items.Single().Expression.AcceptAsync<CSSyntax.IdentifierNameSyntax>(_triviaConvertingVisitor);

                    selectOrGroup = SyntaxFactory.GroupClause(identifierNameSyntax, await GetByExpressionAsync(gcs));

                    break;

                case VBSyntax.SelectClauseSyntax scs:
                    selectOrGroup = await ConvertSelectClauseSyntaxAsync(scs);

                    if (nestedClause != null) {
                        var newId = SyntaxFactory.Identifier("inner");
                    }

                    break;

                default:
                    throw new NotImplementedException($"Clause kind '{clauseEnd.Kind()}' is not yet implemented");
            }

            return SyntaxFactory.QueryBody(convertedClauses, selectOrGroup, queryContinuation);
        }

        private SyntaxList<CSSyntax.QueryClauseSyntax> AddAggregateFunctionLetClauses(GroupByClauseSyntax gcs, CSSyntax.ExpressionSyntax groupExpression, SyntaxList<CSSyntax.QueryClauseSyntax> continuationClauses)
        {
            return GetAggregateFunctionIdentifiers(gcs)
               .Select(id => CreateLetClause(id, groupExpression))
               .Aggregate(continuationClauses, (current, letClause) => current.Add(letClause));
        }

        private static SyntaxList<CSSyntax.QueryClauseSyntax> AddGroupKeyLetClauses(CSSyntax.ExpressionSyntax groupExpression, IReadOnlyCollection<string> groupKeyIds, SyntaxList<CSSyntax.QueryClauseSyntax> continuationClauses)
        {
            var keyIdentifier = SyntaxFactory.IdentifierName("Key");

            var groupKeyMacEx = SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression,
                groupExpression, keyIdentifier);

            var getLetClauseMac = groupKeyIds.Count == 1
                ? (Func<string, CSSyntax.MemberAccessExpressionSyntax>) (_ => groupKeyMacEx)
                : name => SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression, groupKeyMacEx, SyntaxFactory.IdentifierName(name));

            continuationClauses = groupKeyIds
               .Select(groupKeyId => SyntaxFactory.LetClause(groupKeyId, getLetClauseMac(groupKeyId)))
               .Aggregate(continuationClauses, (current, letClause) => current.Add(letClause));

            return continuationClauses;
        }

        private static SyntaxList<CSSyntax.QueryClauseSyntax> AddGroupLetClause(SyntaxToken? vbGroupAlias, CSSyntax.ExpressionSyntax groupExpression, SyntaxList<CSSyntax.QueryClauseSyntax> continuationClauses)
        {
            var groupLetIdentifier = SyntaxFactory.Identifier("Group");
            var name = vbGroupAlias ?? groupLetIdentifier;

            var assEnumerableIdentifier = SyntaxFactory.IdentifierName("AsEnumerable");

            var continuationGroupLetClause = CreateLetClause(assEnumerableIdentifier, groupExpression, name);
            continuationClauses = continuationClauses.Add(continuationGroupLetClause);

            return continuationClauses;
        }

        private SyntaxToken? GetVbGroupAlias(GroupByClauseSyntax gcs)
        {
            var aliasToken = gcs.AggregationVariables
               .Select(v => v.Aggregation switch
                {
                    VBSyntax.GroupAggregationSyntax => v.NameEquals?.Identifier.Identifier,
                    var _ => default
                })
               .SingleOrDefault(x => x != null);

            return aliasToken != null
                ? CommonConversions.ConvertIdentifier(aliasToken.Value)
                : null;
        }

        private static CSSyntax.LetClauseSyntax CreateLetClause(SyntaxToken macAndLetClauseId, CSSyntax.ExpressionSyntax expression)
        {
            var name = SyntaxFactory.IdentifierName(macAndLetClauseId);

            return CreateLetClause(name, expression, macAndLetClauseId);
        }

        private static CSSyntax.LetClauseSyntax CreateLetClause(CSSyntax.SimpleNameSyntax name, CSSyntax.ExpressionSyntax expression, SyntaxToken letClauseId)
        {
           var mac = SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression, expression, name);
           var invExp = SyntaxFactory.InvocationExpression(mac);
           var letClause = SyntaxFactory.LetClause(letClauseId, invExp);

           return letClause;
        }

        private static CSSyntax.QueryContinuationSyntax CreateGroupByContinuation(SyntaxList<CSSyntax.QueryClauseSyntax> convertedClauses, CSSyntax.QueryBodySyntax body,
            SyntaxToken groupName)
        {
            var queryBody = convertedClauses.Any()
                ? SyntaxFactory.QueryBody(convertedClauses, body?.SelectOrGroup, null)
                : SyntaxFactory.QueryBody(body?.SelectOrGroup);

            return SyntaxFactory.QueryContinuation(groupName, queryBody);
        }

        private async Task<IEnumerable<CSSyntax.ExpressionSyntax>> GetLinqArgumentsAsync(SyntaxToken reusableCsFromId,
            VBSyntax.QueryClauseSyntax linqQuery)
        {
            switch (linqQuery) {
                case VBSyntax.DistinctClauseSyntax _:
                    return Enumerable.Empty<CSSyntax.ExpressionSyntax>();
                case VBSyntax.PartitionClauseSyntax pcs:
                    return new[] {await pcs.Count.AcceptAsync<CSSyntax.ExpressionSyntax>(_triviaConvertingVisitor)};
                case VBSyntax.PartitionWhileClauseSyntax pwcs: {
                    var lambdaParam = SyntaxFactory.Parameter(reusableCsFromId);
                    var lambdaBody = await pwcs.Condition.AcceptAsync<CSSyntax.ExpressionSyntax>(_triviaConvertingVisitor);
                    return new CSSyntax.ExpressionSyntax[] {SyntaxFactory.SimpleLambdaExpression(lambdaParam, lambdaBody)};
                }
                default:
                    throw new ArgumentOutOfRangeException(nameof(linqQuery), linqQuery.Kind(), null);
            }
        }

        private static string GetLinqMethodName(VBSyntax.QueryClauseSyntax queryEnd)
        {
            switch (queryEnd) {
                case VBSyntax.DistinctClauseSyntax _:
                    return nameof(Enumerable.Distinct);
                case VBSyntax.PartitionClauseSyntax pcs:
                    return pcs.SkipOrTakeKeyword.IsKind(Microsoft.CodeAnalysis.VisualBasic.SyntaxKind.SkipKeyword) ? nameof(Enumerable.Skip) : nameof(Enumerable.Take);
                case VBSyntax.PartitionWhileClauseSyntax pwcs:
                    return pwcs.SkipOrTakeKeyword.IsKind(Microsoft.CodeAnalysis.VisualBasic.SyntaxKind.SkipKeyword) ? nameof(Enumerable.SkipWhile) : nameof(Enumerable.TakeWhile);
                default:
                    throw new ArgumentOutOfRangeException(nameof(queryEnd), queryEnd.Kind(), null);
            }
        }

        private static bool RequiresMethodInvocation(VBSyntax.QueryClauseSyntax queryClauseSyntax)
        {
            return queryClauseSyntax is VBSyntax.PartitionClauseSyntax
                   || queryClauseSyntax is VBSyntax.PartitionWhileClauseSyntax
                   || queryClauseSyntax is VBSyntax.DistinctClauseSyntax;
        }

        /// <summary>
        /// In VB, multiple selects work like Let clauses, but the last one needs to become the actual select (its name is discarded)
        /// </summary>
        private static bool RequiredContinuation(VBSyntax.QueryClauseSyntax queryClauseSyntax, int clausesAfter) => queryClauseSyntax is VBSyntax.GroupByClauseSyntax
                || queryClauseSyntax is VBSyntax.SelectClauseSyntax sc && (sc.Variables.Any(v => v.NameEquals is null) || clausesAfter == 0);

        private async Task<CSSyntax.FromClauseSyntax> ConvertFromClauseSyntaxAsync(VBSyntax.FromClauseSyntax vbFromClause)
        {
            var collectionRangeVariableSyntax = vbFromClause.Variables.Single();
            var expression = await collectionRangeVariableSyntax.Expression.AcceptAsync<CSSyntax.ExpressionSyntax>(_triviaConvertingVisitor);
            var parentOperation = _semanticModel.GetOperation(collectionRangeVariableSyntax.Expression)?.Parent;
            if (parentOperation != null && parentOperation.IsImplicit && parentOperation is IInvocationOperation io &&
                io.TargetMethod.MethodKind == MethodKind.ReducedExtension && io.TargetMethod.Name == nameof(Enumerable.AsEnumerable)) {
                expression = SyntaxFactory.InvocationExpression(ValidSyntaxFactory.MemberAccess(expression, io.TargetMethod.Name), SyntaxFactory.ArgumentList());
            }
            var fromClauseSyntax = SyntaxFactory.FromClause(
                CommonConversions.ConvertIdentifier(collectionRangeVariableSyntax.Identifier.Identifier),
                expression);
            return fromClauseSyntax;
        }

        private async Task<CSSyntax.FromClauseSyntax> ConvertAggregateToFromClauseSyntaxAsync(VBSyntax.AggregateClauseSyntax vbAggClause)
        {
            var collectionRangeVariableSyntax = vbAggClause.Variables.Single();
            var fromClauseSyntax = SyntaxFactory.FromClause(
                CommonConversions.ConvertIdentifier(collectionRangeVariableSyntax.Identifier.Identifier),
                await collectionRangeVariableSyntax.Expression.AcceptAsync<CSSyntax.ExpressionSyntax>(_triviaConvertingVisitor));
            return fromClauseSyntax;
        }

        private async Task<CSSyntax.SelectClauseSyntax> ConvertSelectClauseSyntaxAsync(VBSyntax.SelectClauseSyntax vbSelectClause)
        {
            var selectedVariables = await vbSelectClause.Variables.SelectAsync(async v => {
                    var nameEquals = await v.NameEquals.AcceptAsync<CSSyntax.NameEqualsSyntax>(_triviaConvertingVisitor);
                    var expression = await v.Expression.AcceptAsync<CSSyntax.ExpressionSyntax>(_triviaConvertingVisitor);
                    return SyntaxFactory.AnonymousObjectMemberDeclarator(nameEquals, expression);
                });

            if (selectedVariables.Count() == 1)
                return SyntaxFactory.SelectClause(selectedVariables.Single().Expression);
            return SyntaxFactory.SelectClause(SyntaxFactory.AnonymousObjectCreationExpression(SyntaxFactory.SeparatedList(selectedVariables)));
        }

        /// <summary>
        /// TODO: In the case of multiple Froms and no Select, VB returns an anonymous type containing all the variables created by the from clause
        /// </summary>
        private static CSSyntax.SelectClauseSyntax CreateDefaultSelectClause(SyntaxToken reusableCsFromId)
        {
            return SyntaxFactory.SelectClause(SyntaxFactory.IdentifierName(reusableCsFromId));
        }

        private Task<CSSyntax.QueryClauseSyntax> ConvertQueryBodyClauseAsync(VBSyntax.QueryClauseSyntax node)
        {
            return node switch {
                VBSyntax.FromClauseSyntax x => ConvertFromQueryClauseSyntaxAsync(x),
                VBSyntax.JoinClauseSyntax x => ConvertJoinClauseAsync(x),
                VBSyntax.SelectClauseSyntax x => ConvertSelectClauseAsync(x),
                VBSyntax.LetClauseSyntax x => ConvertLetClauseAsync(x),
                VBSyntax.OrderByClauseSyntax x => ConvertOrderByClauseAsync(x),
                VBSyntax.WhereClauseSyntax x => ConvertWhereClauseAsync(x),
                _ => throw new NotImplementedException($"Conversion for query clause with kind '{node.Kind()}' not implemented")
            };

            async Task<CSSyntax.QueryClauseSyntax> ConvertFromQueryClauseSyntaxAsync(VBSyntax.FromClauseSyntax x) => await ConvertFromClauseSyntaxAsync(x);
        }

        private async Task<CSSyntax.ExpressionSyntax> GetByExpressionAsync(VBSyntax.GroupByClauseSyntax gs)
        {
            var groupExpressions = (await gs.Keys.SelectAsync(async k =>
                (Vb: (NameEqualsId: k.NameEquals?.Identifier, k.Expression),
                    Cs: await k.Expression.AcceptAsync<CSSyntax.ExpressionSyntax>(_triviaConvertingVisitor)))).ToList();
            return (groupExpressions.Count == 1) ? groupExpressions.Single().Cs : CreateAnonymousType(groupExpressions);
        }

        private static CSSyntax.ExpressionSyntax CreateAnonymousType(List<((ModifiedIdentifierSyntax NameEqualsId, ExpressionSyntax Expression) Vb,
            CSSyntax.ExpressionSyntax Cs)> groupExpressions)
        {
            var anonObjectMemberDeclarators = groupExpressions.Select(CreateAnonymousMember);
            var initializers = SyntaxFactory.SeparatedList(anonObjectMemberDeclarators);
            return SyntaxFactory.AnonymousObjectCreationExpression(initializers);
        }

        private static CSSyntax.AnonymousObjectMemberDeclaratorSyntax CreateAnonymousMember(((ModifiedIdentifierSyntax NameEqualsId, ExpressionSyntax Expression) Vb,
            CSSyntax.ExpressionSyntax Cs) expr, int i)
        {
            var (vb, cs) = expr;
            var name = SyntaxFactory.Identifier(vb.NameEqualsId?.Identifier.Text ?? vb.Expression.ExtractAnonymousTypeMemberName()?.Text ?? ("key" + i));
            return SyntaxFactory.AnonymousObjectMemberDeclarator(SyntaxFactory.NameEquals(SyntaxFactory.IdentifierName(name)), cs);
        }

        private SyntaxToken GetGroupIdentifier(bool isContinuation, SyntaxToken? vbGroupAlias)
        {
            var groupText = isContinuation
                ? "g" != vbGroupAlias.GetValueOrDefault().ValueText ? "g" : "g1"
                : "Group";

            var groupIdentifier = CommonConversions.CsEscapedIdentifier(groupText);

            return CommonConversions.ConvertIdentifier(groupIdentifier);
        }

        private IEnumerable<SyntaxToken> GetAggregateFunctionIdentifiers(GroupByClauseSyntax gcs)
        {
            var names = gcs.AggregationVariables
               .Select(v => v.Aggregation)
               .OfType<VBSyntax.FunctionAggregationSyntax>()
               .Select(f => CommonConversions.ConvertIdentifier(f.FunctionName));

            return names;
        }

        private static IEnumerable<string> GetGroupKeyIdentifiers(VBSyntax.GroupByClauseSyntax gs)
        {
            return gs.Keys.Select(k => k.NameEquals?.Identifier.Identifier.Text)
                .Where(x => x != null);
        }

        private async Task<CSSyntax.QueryClauseSyntax> ConvertWhereClauseAsync(VBSyntax.WhereClauseSyntax ws)
        {
            return SyntaxFactory.WhereClause(await ws.Condition.AcceptAsync<CSSyntax.ExpressionSyntax>(_triviaConvertingVisitor));
        }

        private async Task<CSSyntax.QueryClauseSyntax> ConvertSelectClauseAsync(VBSyntax.SelectClauseSyntax sc)
        {
            var singleVariable = sc.Variables.Single();
            var identifier = CommonConversions.ConvertIdentifier(singleVariable.NameEquals.Identifier.Identifier);
            return SyntaxFactory.LetClause(identifier, await singleVariable.Expression.AcceptAsync<CSSyntax.ExpressionSyntax>(_triviaConvertingVisitor));
        }

        private async Task<CSSyntax.QueryClauseSyntax> ConvertLetClauseAsync(VBSyntax.LetClauseSyntax ls)
        {
            var singleVariable = ls.Variables.Single();
            var identifier = CommonConversions.ConvertIdentifier(singleVariable.NameEquals.Identifier.Identifier);
            return SyntaxFactory.LetClause(identifier, await singleVariable.Expression.AcceptAsync<CSSyntax.ExpressionSyntax>(_triviaConvertingVisitor));
        }

        private async Task<CSSyntax.QueryClauseSyntax> ConvertOrderByClauseAsync(VBSyntax.OrderByClauseSyntax os)
        {
            var orderingSyntaxs = await os.Orderings.SelectAsync(async o => await o.AcceptAsync<CSSyntax.OrderingSyntax>(_triviaConvertingVisitor));
            return SyntaxFactory.OrderByClause(SyntaxFactory.SeparatedList(orderingSyntaxs));
        }

        private async Task<CSSyntax.QueryClauseSyntax> ConvertJoinClauseAsync(VBSyntax.JoinClauseSyntax js)
        {
            var variable = js.JoinedVariables.Single();
            var joinLhs = SingleExpression(await js.JoinConditions.SelectAsync(async c => await c.Left.AcceptAsync<CSSyntax.ExpressionSyntax>(_triviaConvertingVisitor)));
            var joinRhs = SingleExpression(await js.JoinConditions.SelectAsync(async c => await c.Right.AcceptAsync<CSSyntax.ExpressionSyntax>(_triviaConvertingVisitor)));
            var convertIdentifier = CommonConversions.ConvertIdentifier(variable.Identifier.Identifier);
            var expressionSyntax = await variable.Expression.AcceptAsync<CSSyntax.ExpressionSyntax>(_triviaConvertingVisitor);

            CSSyntax.JoinIntoClauseSyntax joinIntoClauseSyntax = null;
            if (js is VBSyntax.GroupJoinClauseSyntax gjs) {
                joinIntoClauseSyntax = gjs.AggregationVariables
                    .Where(a => a.Aggregation is VBSyntax.GroupAggregationSyntax)
                    .Select(a => SyntaxFactory.JoinIntoClause(CommonConversions.ConvertIdentifier(a.NameEquals.Identifier.Identifier)))
                    .SingleOrDefault();
            }
            return SyntaxFactory.JoinClause(null, convertIdentifier, expressionSyntax, joinLhs, joinRhs, joinIntoClauseSyntax);
        }

        private CSSyntax.ExpressionSyntax SingleExpression(IReadOnlyCollection<CSSyntax.ExpressionSyntax> expressions)
        {
            if (expressions.Count == 1) return expressions.Single();
            return SyntaxFactory.AnonymousObjectCreationExpression(SyntaxFactory.SeparatedList(expressions.Select((e, i) =>
                SyntaxFactory.AnonymousObjectMemberDeclarator(SyntaxFactory.NameEquals($"key{i}"), e)
            )));
        }
    }
}
