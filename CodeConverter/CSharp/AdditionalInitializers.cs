using System.Collections.Generic;
using System.Linq;
using ICSharpCode.CodeConverter.Util;
using ICSharpCode.CodeConverter.Util.FromRoslyn;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using VBSyntax = Microsoft.CodeAnalysis.VisualBasic.Syntax;

namespace ICSharpCode.CodeConverter.CSharp
{
    internal class AdditionalInitializers
    {
        private readonly bool _shouldAddInstanceConstructor;
        private readonly bool _shouldAddStaticConstructor;

        public AdditionalInitializers(VBSyntax.TypeBlockSyntax typeSyntax, INamedTypeSymbol namedTypeSybol,
            Compilation vbCompilation)
        {
            var (instanceConstructors, staticConstructors) = namedTypeSybol.GetDeclaredConstructorsInAllParts();
            var isBestPartToAddParameterlessConstructor = IsBestPartToAddParameterlessConstructor(typeSyntax, namedTypeSybol);
            _shouldAddInstanceConstructor = !instanceConstructors.Any() && isBestPartToAddParameterlessConstructor;
            _shouldAddStaticConstructor = !staticConstructors.Any() && isBestPartToAddParameterlessConstructor;
            IsBestPartToAddTypeInit = isBestPartToAddParameterlessConstructor;
            HasInstanceConstructorsOutsideThisPart = instanceConstructors.Any(c => c.DeclaringSyntaxReferences.Any(
                reference => !typeSyntax.OverlapsWith(reference)
            )) || !instanceConstructors.Any() && !isBestPartToAddParameterlessConstructor;
            RequiresInitializeComponent = namedTypeSybol.IsDesignerGeneratedTypeWithInitializeComponent(vbCompilation);
        }

        public bool HasInstanceConstructorsOutsideThisPart { get; }
        public bool IsBestPartToAddTypeInit { get; }
        public bool RequiresInitializeComponent { get; }

        public List<Assignment> AdditionalStaticInitializers { get; } = new List<Assignment>();
        public List<Assignment> AdditionalInstanceInitializers { get; } = new List<Assignment>();

        public IReadOnlyCollection<MemberDeclarationSyntax> WithAdditionalInitializers(List<MemberDeclarationSyntax> convertedMembers, SyntaxToken parentTypeName)
        {
            var (rootInstanceConstructors, rootStaticConstructors) = convertedMembers.OfType<ConstructorDeclarationSyntax>()
                .Where(cds => !cds.Initializer.IsKind(SyntaxKind.ThisConstructorInitializer))
                .SplitOn(cds => cds.IsInStaticCsContext());

            convertedMembers = WithAdditionalInitializers(convertedMembers, parentTypeName, AdditionalInstanceInitializers, SyntaxFactory.TokenList(SyntaxFactory.Token(SyntaxKind.PublicKeyword)), rootInstanceConstructors, _shouldAddInstanceConstructor, RequiresInitializeComponent);

            convertedMembers = WithAdditionalInitializers(convertedMembers, parentTypeName,
                AdditionalStaticInitializers, SyntaxFactory.TokenList(SyntaxFactory.Token(SyntaxKind.StaticKeyword)), rootStaticConstructors, _shouldAddStaticConstructor, false);

            return convertedMembers;
        }

        private List<MemberDeclarationSyntax> WithAdditionalInitializers(List<MemberDeclarationSyntax> convertedMembers,
            SyntaxToken convertIdentifier, IReadOnlyCollection<Assignment> additionalInitializers,
            SyntaxTokenList modifiers, IEnumerable<ConstructorDeclarationSyntax> constructorsEnumerable, bool addConstructor, bool addedConstructorRequiresInitializeComponent)
        {
            if (!additionalInitializers.Any() && (!addConstructor || !addedConstructorRequiresInitializeComponent)) return convertedMembers;

            var constructors = new HashSet<ConstructorDeclarationSyntax>(constructorsEnumerable);
            var ctorsWithIndex = convertedMembers
               .Select((member, index) => new {Member = member as ConstructorDeclarationSyntax, Index = index})
               .Where(memberWithIndex => memberWithIndex.Member != null)
               .ToList();

            var filteredCtorsWithIndex = ctorsWithIndex
               .Where(memberWithIndex => constructors.Contains(memberWithIndex.Member))
               .ToList();

            convertedMembers = convertedMembers
               .Except(constructors)
               .ToList();

            if (addConstructor)
            {
                var statements = new List<StatementSyntax>();
                if (addedConstructorRequiresInitializeComponent)
                {
                    var icIdentifier = SyntaxFactory.IdentifierName("InitializeComponent");
                    var icInvocation = SyntaxFactory.InvocationExpression(icIdentifier);
                    statements.Add(SyntaxFactory.ExpressionStatement(icInvocation));
                }

                var ctorDecl = SyntaxFactory.ConstructorDeclaration(convertIdentifier);
                var ctorBody = SyntaxFactory.Block(statements.ToArray());

                var newCtorDecl = ctorDecl
                   .WithBody(ctorBody)
                   .WithModifiers(modifiers);

                filteredCtorsWithIndex.Add(new
                {
                    Member = newCtorDecl,
                    Index = filteredCtorsWithIndex.LastOrDefault()?.Index + 1
                            ?? ctorsWithIndex.LastOrDefault()?.Index + 1
                            ?? 0
                });
            }

            foreach (var filteredCtorWithIndex in filteredCtorsWithIndex)
            {
                var newConstructor = WithAdditionalInitializers(filteredCtorWithIndex.Member, additionalInitializers);
                convertedMembers.Insert(filteredCtorWithIndex.Index, newConstructor);
            }

            return convertedMembers;
        }

        private ConstructorDeclarationSyntax WithAdditionalInitializers(ConstructorDeclarationSyntax oldConstructor,
            IReadOnlyCollection<Assignment> additionalConstructorAssignments)
        {
            var preInitializerStatements = CreateAssignmentStatement(additionalConstructorAssignments.Where(x => !x.PostAssignment));
            var postInitializerStatements = CreateAssignmentStatement(additionalConstructorAssignments.Where(x => x.PostAssignment));
            var oldConstructorBody = oldConstructor.Body ?? SyntaxFactory.Block(SyntaxFactory.ExpressionStatement(oldConstructor.ExpressionBody.Expression));
            var newConstructor = oldConstructor.WithBody(oldConstructorBody.WithStatements(
                oldConstructorBody.Statements.InsertRange(0, preInitializerStatements).AddRange(postInitializerStatements)));

            return newConstructor;
        }

        private static List<ExpressionStatementSyntax> CreateAssignmentStatement(IEnumerable<Assignment> additionalConstructorAssignments)
        {
            return additionalConstructorAssignments.Select(assignment =>
                            SyntaxFactory.ExpressionStatement(SyntaxFactory.AssignmentExpression(
                                assignment.AssignmentKind, assignment.Field, assignment.Initializer))
                        ).ToList();
        }

        private static bool IsBestPartToAddParameterlessConstructor(VBSyntax.TypeBlockSyntax typeSyntax, INamedTypeSymbol namedTypeSybol)
        {
            if (namedTypeSybol == null) return false;

            var bestPartToAddTo = namedTypeSybol.DeclaringSyntaxReferences
                .OrderByDescending(l => l.SyntaxTree.FilePath?.IsGeneratedFile() == false).ThenBy(l => l.GetSyntax() is VBSyntax.TypeBlockSyntax tbs && HasAttribute(tbs, "DesignerGenerated"))
                .First();
            return typeSyntax.OverlapsWith(bestPartToAddTo);
        }

        private static bool HasAttribute(VBSyntax.TypeBlockSyntax tbs, string attributeName)
        {
            return tbs.BlockStatement.AttributeLists.Any(list => list.Attributes.Any(a => a.Name.GetText().ToString().Contains(attributeName)));
        }
    }
}