using Sitecore.Commerce.Engine.Connect;
using Sitecore.Commerce.Engine.Connect.Search.ComputedFields;
using Sitecore.ContentSearch;
using Sitecore.Data;
using Sitecore.Data.Items;
using Sitecore.Diagnostics;
using System.Collections.Generic;

namespace Sitecore.Support.Commerce.Engine.Connect.Search.ComputedFields
{
    public class CommerceSearchItemTypeField : BaseCommerceComputedField
    {
        private static readonly IEnumerable<ID> _validTemplates = new List<ID>
    {
        CommerceConstants.KnownTemplateIds.CommerceProductTemplate,
        CommerceConstants.KnownTemplateIds.CommerceProductVariantTemplate,
        CommerceConstants.KnownTemplateIds.CommerceCategoryTemplate,
        CommerceConstants.KnownTemplateIds.CommerceNavigationItemTemplate,
        CommerceConstants.KnownTemplateIds.CommerceCatalogTemplate
    };

        protected override IEnumerable<ID> ValidTemplates => _validTemplates;

        public override object ComputeValue(IIndexable indexable)
        {
            Assert.ArgumentNotNull((object)indexable, "indexable");
            string result = "Unknown";
            Item validatedItem = GetValidatedItem(indexable);
            if (validatedItem != null)
            {
                result = GetItemType(validatedItem);
            }
            return result;
        }
    }
}
