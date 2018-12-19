namespace Catalyst.PluginTester.DummyImplementations
{
    using Catalyst.DataProcessing.Shared.Utilities.Context;

    public class TestProcessingContextWrapperFactory : IProcessingContextWrapperFactory
    {
        private readonly TestProcessingContextWrapper testProcessingContextWrapper;

        public TestProcessingContextWrapperFactory(TestProcessingContextWrapper testProcessingContextWrapper)
        {
            this.testProcessingContextWrapper = testProcessingContextWrapper ?? throw new System.ArgumentNullException(nameof(testProcessingContextWrapper));
        }

        public IProcessingContextWrapper CreateProcessingContextWrapper()
        {
            return this.testProcessingContextWrapper;
        }
    }
}
