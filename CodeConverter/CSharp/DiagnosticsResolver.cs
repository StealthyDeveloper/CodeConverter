using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ICSharpCode.CodeConverter.Shared;
using ICSharpCode.CodeConverter.Util;
using ICSharpCode.CodeConverter.Util.FromRoslyn;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Editing;
using Microsoft.CodeAnalysis.FindSymbols;
using Microsoft.CodeAnalysis.Formatting;

namespace ICSharpCode.CodeConverter.CSharp
{
    public class DiagnosticsResolver
    {
        private static readonly string[] UnusedDiagnosticIds = { "CS0168", "CS0169", "CS0414", "CS0219" };

        public async Task<SyntaxNode> ResolveDiagnosticsAsync(Document document)
        {
            var compilation = await document.Project.GetCompilationAsync();
            var tree = await document.GetSyntaxTreeAsync();
            var root = await tree.GetRootAsync();

            if (compilation.Options.GeneralDiagnosticOption != ReportDiagnostic.Error) return root;

            var designerGeneratedName = compilation.DesignerGeneratedAttributeType().GetFullMetadataName();
            var semanticModel = compilation.GetSemanticModel(tree, true);
            var diagnostics = compilation.GetDiagnostics();

            var icNodes = root.DescendantNodes().OfType<MethodDeclarationSyntax>()
                   .Where(decl => decl.Identifier.ValueText == "InitializeComponent");

            var fieldDeclarationSyntaxes = diagnostics
               .Where(d => d.Id == "CS0649")
               .Where(d => d.Location?.SourceTree == tree)
               .Select(d => root.FindNode(d.Location.SourceSpan))
               .Select(node => node.FirstAncestorOrSelf<FieldDeclarationSyntax>());

            var unassignedFields = await fieldDeclarationSyntaxes
               .Where(node => node?.FirstAncestorOrSelf<ClassDeclarationSyntax>()?.AttributeLists
                   .Any(alist => alist.Attributes.Any(a => a.Name.ToString()
                       .Contains(designerGeneratedName))) ?? false)
               .SelectAsync(async node => await GetFieldTypeTupleAsync(document, semanticModel, node));

            var rootWithAssignments = root.ReplaceNodes(icNodes, (o, _) => ComputeAssignmentReplacements(o, unassignedFields));

            var unusedVariableAndFieldNodes = diagnostics
               .Where(d => UnusedDiagnosticIds.Contains(d.Id))
               .Where(d => d.Location?.SourceTree == tree)
               .Select(d => rootWithAssignments.FindNode(d.Location.SourceSpan))
               .ToList();

            var unusedVariableAndFieldDeclNodes = unusedVariableAndFieldNodes
               .Select(node => (SyntaxNode)node.FirstAncestorOrSelf<LocalDeclarationStatementSyntax>() ?? node.FirstAncestorOrSelf<FieldDeclarationSyntax>())
               .WithoutNulls();
            
            var unusedVariableAndFieldIdNodes = unusedVariableAndFieldNodes
               .Where(node => node is CatchDeclarationSyntax) 
               .Select(node => ((CatchDeclarationSyntax)node).Identifier)
               .WithoutNulls();

            var noUnusedDeclNodesRoot = rootWithAssignments.RemoveNodes(unusedVariableAndFieldDeclNodes, SyntaxRemoveOptions.KeepNoTrivia);
            var noUnusedIdNodesRoot = noUnusedDeclNodesRoot.ReplaceTokens(unusedVariableAndFieldIdNodes, (o, r) => SyntaxFactory.Token(SyntaxKind.None));

            return noUnusedIdNodesRoot;
        }

        private static async Task<(FieldDeclarationSyntax node, TypeSyntax fieldAssignmentType)> GetFieldTypeTupleAsync(Document document, SemanticModel semanticModel, FieldDeclarationSyntax node)
        {
            var fieldsymbol = ModelExtensions.GetSymbolInfo(semanticModel, node.Declaration.Type).Symbol as ITypeSymbol;
            var fieldAssignmentType = fieldsymbol.IsInterfaceType()
                ? await GetFieldInterfaceImplementationNameAsync(document, fieldsymbol)
                : node.Declaration.Type;

            return (node, fieldAssignmentType);
        }


        private static async Task<NameSyntax> GetFieldInterfaceImplementationNameAsync(Document document, ITypeSymbol fieldsymbol)
        {
            var implementations = await SymbolFinder.FindImplementationsAsync(fieldsymbol, document.Project.Solution);
            var impl = implementations.First() as ITypeSymbol;
            var syntaxGenerator = SyntaxGenerator.GetGenerator(document.Project);
            return (NameSyntax)syntaxGenerator.TypeExpression(impl);
        }

        private static SyntaxNode ComputeAssignmentReplacements(MethodDeclarationSyntax icNode, IEnumerable<(FieldDeclarationSyntax node,
            TypeSyntax fieldAssignmentType)> unassignedFields)
        {
            var expressions = new List<ExpressionStatementSyntax>();
            foreach (var (field, fieldAssignmentType) in unassignedFields)
            {
                expressions.AddRange(field.Declaration.Variables.Select(variable =>
                {
                    var lhs = SyntaxFactory.IdentifierName(variable.Identifier.Text);
                    var rhs = SyntaxFactory.ObjectCreationExpression(fieldAssignmentType,
                        SyntaxFactory.ArgumentList(), null);
                    var assignment = SyntaxFactory.AssignmentExpression(SyntaxKind.SimpleAssignmentExpression, lhs,
                        rhs);
                    return SyntaxFactory.ExpressionStatement(assignment);
                }));
            }

            var newStatements = icNode.Body.Statements.InsertRange(0, expressions);
            var newBody = icNode.Body.WithStatements(newStatements);
            var newicNode = icNode.WithBody(newBody).WithAdditionalAnnotations(Formatter.Annotation);

            return newicNode;
        }
    }
}
