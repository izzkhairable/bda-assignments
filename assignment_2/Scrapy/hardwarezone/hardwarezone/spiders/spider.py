import scrapy


class HardwarezoneSpider(scrapy.Spider):
    name = "hardwarezone"

    start_urls = [
        "https://forums.hardwarezone.com.sg/forums/pc-gaming.382/",
    ]

    def parse(self, response):
        # There can be multiple groups of threads in a single page so loop through it get the links and use it in response.follow
        for topic_list in response.xpath(
            '//div[has-class("structItemContainer-group")]'
        ):
            for topic in topic_list.xpath("div"):
                yield response.follow(
                    topic.xpath("div[2]/div/a/@href").get(), self.parse
                )

        # This will only run when it is in the individual/single thread page
        if not response.xpath("//title/text()").get().startswith("PC Gaming |"):
            for post in response.xpath(
                '//article[@class="message message--post js-post js-inlineModContainer  "]'
            ):
                # As some of the authors for the post moderators, their name is hidden under a additional span tag
                author = []
                # If author name text isn't found that means it is under the span
                if post.xpath('div//h4[@class="message-name"]/a/text()').get() is None:
                    author = post.xpath(
                        'div//h4[@class="message-name"]/a/span/text()'
                    ).get()

                # Else get the name authorname text without the span
                else:
                    author = post.xpath("div//h4[@class='message-name']/a/text()").get()

                # added date_posted for a thread as an additional data
                yield {
                    "title": post.xpath('//h1[@class="p-title-value"]/text()').get(),
                    "date_thread_started": post.xpath(
                        '//article[contains(@class, "message message--post")][1]//time/text()'
                    ).get(),
                    "author": author,
                    "content": " ".join(
                        post.xpath(
                            'div//div[contains(@class, "message-content js-messageContent")]//div[@class="bbWrapper"]/text()'
                        ).getall()
                    ),
                    "author_date_joined": post.xpath(
                        "div//div[@class='message-userExtras']/dl[1]/dd/text()"
                    ).get(),
                    "author_user_title": post.xpath(
                        "div//h5[@class='userTitle message-userTitle']/text()"
                    ).get(),
                    "author_number_of_posts": post.xpath(
                        "div//div[@class='message-userExtras']/dl[2]/dd/text()"
                    ).get(),
                    "author_reaction_score": post.xpath(
                        "div//div[@class='message-userExtras']/dl[3]/dd/text()"
                    ).get(),
                }

            next_page = response.xpath(
                '//a[has-class("pageNav-jump pageNav-jump--next")]/@href'
            ).get()
            if next_page is not None:
                yield response.follow(next_page, self.parse)

        # This will only run when it is at the main pc gaming thread list page
        if response.xpath("//title/text()").get().startswith("PC Gaming |"):
            next_page = response.xpath(
                '//a[has-class("pageNav-jump pageNav-jump--next")]/@href'
            ).get()
            if next_page is not None:
                yield response.follow(next_page, self.parse)
