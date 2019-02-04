namespace DataConverter
{
    using System;
    using System.Collections.Generic;

    public class CaseInsensitiveDictionary<V> : Dictionary<string, V>
    {
        public CaseInsensitiveDictionary()
            : base(StringComparer.OrdinalIgnoreCase)
        {
        }
    }
}
