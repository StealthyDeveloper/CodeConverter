using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.VisualBasic;

namespace ICSharpCode.CodeConverter.Shared
{
    internal class VisualBasicCompiler : ICompiler
    {
        private static readonly Lazy<VisualBasicCompilation> LazyVisualBasicCompilation = new(CreateVisualBasicCompilation);
        private readonly string _rootNamespace;
        private readonly ReportDiagnostic _generalDiagnosticOption;

        // ReSharper disable once UnusedMember.Global - Used via generics
        public VisualBasicCompiler() : this("", ReportDiagnostic.Default)
        {
        }

        public VisualBasicCompiler(string rootNamespace, ReportDiagnostic generalDiagnosticOption)
        {
            _rootNamespace = rootNamespace;
            _generalDiagnosticOption = generalDiagnosticOption;
        }

        public SyntaxTree CreateTree(string text)
        {
            return SyntaxFactory.ParseSyntaxTree(text, ParseOptions, encoding: Encoding.UTF8);
        }

        public Compilation CreateCompilationFromTree(SyntaxTree tree, IEnumerable<MetadataReference> references)
        {
            var withReferences = CreateVisualBasicCompilation(references, _rootNamespace, _generalDiagnosticOption);
            return withReferences.AddSyntaxTrees(tree);
        }

        public static VisualBasicCompilation CreateVisualBasicCompilation(IEnumerable<MetadataReference> references,
            string rootNamespace, ReportDiagnostic generalDiagnosticOption)
        {
            var visualBasicCompilation = LazyVisualBasicCompilation.Value;
            var withReferences = visualBasicCompilation
                .WithOptions(visualBasicCompilation.Options.WithRootNamespace(rootNamespace)
                   .WithGeneralDiagnosticOption(generalDiagnosticOption))
                .WithReferences(visualBasicCompilation.References.Concat(references).Distinct());
            return withReferences;
        }

        private static VisualBasicCompilation CreateVisualBasicCompilation()
        {
            var compilationOptions = CreateCompilationOptions();
            return VisualBasicCompilation.Create("Conversion")
                .WithOptions(compilationOptions);
        }

        public static VisualBasicCompilationOptions CreateCompilationOptions(string rootNamespace = null, ReportDiagnostic generalReportDiagnostic = ReportDiagnostic.Default)
        {
            // Caution: The simplifier won't always remove imports unused by the code
            // Known cases are unresolved usings and overload resolution across namespaces (e.g. System.Data.Where
            // and System.Linq.Where)

            var compilationOptions = new VisualBasicCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                .WithGlobalImports(GlobalImport.Parse(
                    "System",
                    "System.Collections",
                    "System.Collections.Generic",
                    "System.Diagnostics",
                    "System.Globalization",
                    "System.IO",
                    "System.Linq",
                    "System.Reflection",
                    "System.Runtime.CompilerServices",
                    "System.Runtime.InteropServices",
                    "System.Security",
                    "System.Text",
                    "System.Threading.Tasks",
                    "System.Xml.Linq",
                    "Microsoft.VisualBasic"))
                .WithOptionExplicit(true)
                .WithOptionCompareText(false)
                .WithOptionStrict(OptionStrict.Off)
                .WithOptionInfer(true)
                .WithRootNamespace(rootNamespace)
                .WithGeneralDiagnosticOption(generalReportDiagnostic);

            return compilationOptions;
        }

        public static VisualBasicParseOptions ParseOptions { get; } = new(LanguageVersion.Latest);
    }
}