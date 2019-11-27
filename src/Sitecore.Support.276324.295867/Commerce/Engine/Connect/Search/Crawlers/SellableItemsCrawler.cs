using Sitecore.Commerce.Plugin.Catalog;
using System.Collections.Generic;
using Sitecore.Data;
using Sitecore.Commerce.Plugin.ManagedLists;
using Sitecore.Support.Sitecore.Commerce.Engine.Connect.Search.Crawlers;
using Sitecore.Commerce.Engine.Connect;
using Sitecore.Commerce.Engine.Connect.Search;


namespace Sitecore.Support.Commerce.Engine.Connect.Search.Crawlers
{
    public class SellableItemsCrawler : CatalogCrawlerBase<SellableItem>
    {
        private static readonly List<ID> InternalTemplateIds = new List<ID>
        {
            CommerceConstants.KnownTemplateIds.CommerceProductTemplate
        };

        protected override IEnumerable<ID> IndexableTemplateIds => InternalTemplateIds;

        protected override ManagedList GetCatalogEntitiesToIndex(string environment, string listName, int itemsToSkip, int itemsToTake)
        {
            var itemsList = IndexUtility.GetSellableItemsToIndex(environment, listName, itemsToSkip, itemsToTake);
            return itemsList;
        }
    }
}
