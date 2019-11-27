using Sitecore.Commerce.Engine.Connect;
using Sitecore.Commerce.Engine.Connect.Interfaces;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.ComputedFields;
using Sitecore.Data;
using Sitecore.Data.Items;
using Sitecore.Data.Managers;
using Sitecore.Diagnostics;
using System.Collections.Generic;

namespace Sitecore.Support.Commerce.Engine.Connect.Search.ComputedFields
{
    public abstract class BaseCommerceComputedField : IComputedIndexField
    {
        public string FieldName
        {
            get;
            set;
        }

        public string ReturnType
        {
            get;
            set;
        }

        protected abstract IEnumerable<ID> ValidTemplates
        {
            get;
        }

        protected virtual string GetItemType(Item item)
        {
            string result = "Unknown";
            ICommerceSearchManager commerceSearchManager = CommerceTypeLoader.CreateInstance<ICommerceSearchManager>();
            if (commerceSearchManager.IsItemProduct(item))
            {
                result = "SellableItem";
            }
            else if (commerceSearchManager.IsItemCategory(item))
            {
                result = "Category";
            }
            else if (commerceSearchManager.IsItemCatalog(item))
            {
                result = "Catalog";
            }
            else if (commerceSearchManager.IsItemNavigation(item))
            {
                result = "Navigation";
            }
            return result;
        }

        public object ComputeFieldValue(IIndexable indexable)
        {
            return ComputeValue(indexable);
        }

        public abstract object ComputeValue(IIndexable itemToIndex);

        #region modified part of the code - added DescendsFromOrEquals method
        public bool IsItemPartOfValidTemplates(Item item)
        {
            Assert.IsNotNull((object)item, "item");
            foreach (ID validTemplate in ValidTemplates)
            {
                if (TemplateManager.GetTemplate(item).DescendsFromOrEquals(validTemplate))
                {
                    return true;
                }
            }
            return false;
        }
        #endregion


        public Item GetValidatedItem(IIndexable itemToIndex)
        {
            SitecoreIndexableItem val = itemToIndex as SitecoreIndexableItem;
            if (val == null)
            {
                return null;
            }
            Item item = val.Item;
            if (!IsItemPartOfValidTemplates(item) || item.Name == "__Standard Values")
            {
                return null;
            }
            return item;
        }
    }
}
