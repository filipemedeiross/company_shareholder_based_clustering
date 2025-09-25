from django.core.paginator import Page


class CursorPage(Page):
    def __init__(self, object_list, has_next, has_previous, next_cursor, prev_cursor):
        super().__init__(object_list, number=None, paginator=None)
        self.has_next = has_next
        self.has_previous = has_previous
        self.next_cursor = next_cursor
        self.prev_cursor = prev_cursor


class CursorPaginator:
    def __init__(self, queryset, per_page, ordering="id"):
        self.queryset = queryset
        self.per_page = per_page
        self.ordering = ordering

    def page(self, after=None, before=None):
        qs = self.queryset.order_by(self.ordering)

        if after:
            qs = qs.filter(**{f"{self.ordering}__gt": after})
        elif before:
            qs = qs.filter(**{f"{self.ordering}__lt": before}).order_by(f"-{self.ordering}")

        items = list(qs[: self.per_page + 1])
        has_more = len(items) > self.per_page
        if has_more:
            items = items[: self.per_page]

        if before:
            items.reverse()

        next_cursor = None
        prev_cursor = None

        if items:
            if after:
                next_cursor = getattr(items[-1], self.ordering) if has_more else None
                prev_cursor = getattr(items[0], self.ordering)
            elif before:
                next_cursor = getattr(items[-1], self.ordering)  # 👈 agora existe mesmo no before
                prev_cursor = getattr(items[0], self.ordering) if has_more else None
            else:
                next_cursor = getattr(items[-1], self.ordering) if has_more else None

        return CursorPage(
            object_list=items,
            has_next=bool(next_cursor),
            has_previous=bool(prev_cursor),
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )

