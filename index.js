const moment = require("moment-timezone");
const {
    pick,
    map,
    pipe,
    values,
    head,
    identity,
    of,
    keys,
    curry,
    sum,
    flatten,
    not,
    uniq,
    paths,
    reject,
    mergeAll,
    toLower,
    anyPass,
    hasPath,
} = require("ramda");
const { size, isUndefined, isEmpty, toNumber, orderBy: lodashorderby, compact } = require("lodash");
const { from, zip, of: rxof, catchError, throwError, iif, tap } = require("rxjs");
const { concatMap, map: rxmap, filter: rxfilter, reduce: rxreduce, defaultIfEmpty } = require("rxjs/operators");
const { query, where, getDocs, collection, limit, collectionGroup } = require("firebase/firestore");
const { db } = require("./database");
const { logroupby, lokeyby, louniqby, lofilter, pipeLog, loorderby } = require("helpers");
const { get, all, mod, matching } = require("shades");
const { Facebook: RoasFacebook } = require("roasfacebook");

const ipEvents = curry((version, ip, roas_user_id) => {
    let q = query(collection(db, "events"), where(version, "==", ip), where("roas_user_id", "==", roas_user_id));
    return from(getDocs(q)).pipe(rxmap(Kartra.utilities.queryDocs));
});

const Timestamp = {
    toUTCDigit: (timestamp) => {
        let regex_expression = /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$/;

        let date = moment(timestamp, "X").format("YYYY-MM-DD");
        let date_is_valid = regex_expression.test(date);

        if (!date_is_valid) {
            return timestamp / 1000;
        } else {
            return timestamp;
        }
    },
};

const Facebook = {
    ads: {
        details: {
            get: ({ ad_ids = [], user_id, fb_ad_account_id, date } = {}) => {
                let func_name = `Facebook:ads:details`;
                console.log(func_name);

                if (!ad_ids) return throwError(`error:${func_name}:no ad_ids`);
                if (!user_id) return throwError(`error:${func_name}:no user_id`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                return from(ad_ids).pipe(
                    concatMap((ad_meta_data) => {
                        let { ad_id, timestamp } = ad_meta_data;
                        let ad_args = { ad_id, date, user_id, fb_ad_account_id };

                        return Facebook.ad.db.get(ad_args).pipe(
                            concatMap((ad) => iif(() => !isEmpty(ad), rxof(ad), Facebook.ad.api.get(ad_args))),
                            rxmap((ad) => ({ ...ad, timestamp })),
                            rxfilter(pipe(isEmpty, not))
                        );
                    }),
                    defaultIfEmpty([])
                );
            },
        },
    },

    ad: {
        details: (ad) => {
            let func_name = `Facebook:ad:details`;
            console.log(func_name);

            if (ad.details) {
                return {
                    account_id: ad.account_id,
                    asset_id: ad.details.ad_id,
                    asset_name: ad.details.ad_name,
                    campaign_id: ad.details.campaign_id,
                    campaign_name: ad.details.campaign_name,
                    adset_id: ad.details.adset_id,
                    adset_name: ad.details.adset_name,
                    ad_id: ad.details.ad_id,
                    ad_name: ad.details.ad_name,
                    name: ad.details.ad_name,
                };
            } else {
                return {
                    account_id: ad.account_id,
                    asset_id: ad.id,
                    asset_name: ad.name,
                    campaign_id: ad.campaign_id,
                    campaign_name: ad.campaign_name,
                    adset_id: ad.adset_id,
                    adset_name: ad.adset_name,
                    ad_id: ad.id,
                    ad_name: ad.name,
                    name: ad.name,
                };
            }
        },

        db: {
            get: ({ ad_id, date, user_id, fb_ad_account_id } = {}) => {
                let func_name = `Facebook:ad:db:get`;
                console.log(func_name);

                if (!ad_id) return throwError(`error:${func_name}:no ad_id`);
                if (!date) return throwError(`error:${func_name}:no date`);
                if (!user_id) return throwError(`error:${func_name}:no user_id`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                return from(RoasFacebook({ user_id }).ad.get_from_db({ ad_id })).pipe(
                    concatMap(identity),
                    rxfilter((ad) => !isEmpty(ad)),
                    rxmap(Facebook.ad.details),
                    defaultIfEmpty({}),
                    catchError((error) => rxof({ ad_id, error: true }))
                );
            },
        },

        api: {
            get: ({ ad_id, date, user_id, fb_ad_account_id } = {}) => {
                let func_name = `Facebook:ad:api:get`;
                console.log(func_name);

                if (!ad_id) return throwError(`error:${func_name}:no ad_id`);
                if (!date) return throwError(`error:${func_name}:no date`);
                if (!user_id) return throwError(`error:${func_name}:no user_id`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                let facebook = RoasFacebook({ user_id });

                return from(facebook.ad.get({ ad_id, date, fb_ad_account_id })).pipe(
                    rxmap(pipe(values, head)),
                    rxfilter((ad) => !isUndefined(ad.id)),
                    concatMap((ad) => {
                        let adset = Facebook.ad.adset.api.get({ adset_id: ad.adset_id, user_id, date, fb_ad_account_id });
                        let campaign = Facebook.ad.campaign.api.get({ campaign_id: ad.campaign_id, user_id, date, fb_ad_account_id });

                        return zip([adset, campaign]).pipe(
                            rxmap(([{ name: adset_name }, { name: campaign_name }]) => ({ ...ad, adset_name, campaign_name })),
                            rxmap(Facebook.ad.details)
                        );
                    }),
                    defaultIfEmpty({}),
                    catchError((error) => rxof({ ad_id, error: true }))
                );
            },
        },

        adset: {
            api: {
                get: ({ adset_id, user_id, date, fb_ad_account_id } = {}) => {
                    let func_name = `Facebook:ad:adset:api:get`;
                    console.log(func_name);

                    if (!adset_id) return throwError(`error:${func_name}:no adset_id`);
                    if (!date) return throwError(`error:${func_name}:no date`);
                    if (!user_id) return throwError(`error:${func_name}:no user_id`);
                    if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                    let facebook = RoasFacebook({ user_id });

                    return from(facebook.adset.get({ adset_id, date, fb_ad_account_id })).pipe(rxmap(pipe(values, head)), defaultIfEmpty({}));
                },
            },
        },

        campaign: {
            api: {
                get: ({ campaign_id, user_id, date, fb_ad_account_id } = {}) => {
                    let func_name = `Facebook:ad:campaign:api:get`;
                    console.log(func_name);

                    if (!campaign_id) return throwError(`error:${func_name}:no campaign_id`);
                    if (!date) return throwError(`error:${func_name}:no date`);
                    if (!user_id) return throwError(`error:${func_name}:no user_id`);
                    if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                    let facebook = RoasFacebook({ user_id });

                    return from(facebook.campaign.get({ campaign_id, date, fb_ad_account_id })).pipe(rxmap(pipe(values, head)));
                },
            },
        },
    },
};

const Event = {
    ad: {
        id: ({ fb_ad_id, h_ad_id, ad_id, fb_id } = {}) => {
            let func_name = `Event:ad:id`;
            console.log(func_name);

            if (fb_id) {
                console.log("fb_id:", fb_id);
            }

            if (ad_id) {
                return ad_id;
            }

            if (fb_ad_id && h_ad_id) {
                if (fb_ad_id == h_ad_id) {
                    return fb_ad_id;
                }

                if (fb_ad_id !== h_ad_id) {
                    return h_ad_id;
                }
            }

            if (fb_ad_id && !h_ad_id) {
                return fb_ad_id;
            }

            if (h_ad_id && !fb_ad_id) {
                return h_ad_id;
            }
        },
    },

    get_utc_timestamp: (value) => {
        // console.log("get_utc_timestamp");

        let timestamp;

        if (get("created_at_unix_timestamp")(value)) {
            timestamp = get("created_at_unix_timestamp")(value);
            // console.log(timestamp);
            return timestamp;
        }

        if (get("utc_unix_time")(value)) {
            let timestamp = get("utc_unix_time")(value);
            // console.log(timestamp);
            return timestamp;
        }

        if (get("utc_iso_datetime")(value)) {
            let timestamp = pipe(get("utc_unix_time"), (value) => moment(value).unix())(value);
            // console.log(timestamp);
            return timestamp;
        }

        timestamp = get("unix_datetime")(value);
        // console.log(timestamp);

        if (!timestamp) {
            console.log("notimestamp");
            console.log(value);
        }

        return timestamp;
    },
};

const Events = {
    user: {
        get: {
            ipv4: ({ roas_user_id, ip }) => {
                let func_name = "Events:user:get:ipv4";
                console.log(func_name);
                let events_query = query(collection(db, "events"), where("roas_user_id", "==", roas_user_id), where("ipv4", "==", ip));
                return from(getDocs(events_query)).pipe(rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())));
            },

            ipv6: ({ roas_user_id, ip }) => {
                let func_name = "Events:user:get:ipv6";
                console.log(func_name);
                let events_query = query(collection(db, "events"), where("roas_user_id", "==", roas_user_id), where("ipv6", "==", ip));
                return from(getDocs(events_query)).pipe(rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())));
            },
        },
    },
};

const Kartra = {
    utilities: {
        getDates: (startDate, endDate) => {
            const dates = [];
            let currentDate = startDate;
            const addDays = function (days) {
                const date = new Date(this.valueOf());
                date.setDate(date.getDate() + days);
                return date;
            };
            while (currentDate <= endDate) {
                dates.push(currentDate);
                currentDate = addDays.call(currentDate, 1);
            }
            return dates;
        },

        get_dates_range_array: (since, until) => {
            let start_date = pipe(
                split("-"),
                map(toNumber),
                mod(1)((value) => value - 1),
                (value) => new Date(...value)
            )(since);

            let end_date = pipe(
                split("-"),
                map(toNumber),
                mod(1)((value) => value - 1),
                (value) => new Date(...value)
            )(until);

            const dates = pipe(
                ([start_date, end_date]) => Rules.utilities.getDates(start_date, end_date),
                mod(all)((date) => moment(date, "YYYY-MM-DD").format("YYYY-MM-DD"))
            )([start_date, end_date]);

            return dates;
        },

        date_pacific_time: (date, timezone = "America/Los_Angeles") => moment(date).tz(timezone),

        date_start_end_timestamps: (
            start = moment().format("YYYY-MM-DD"),
            end = moment().format("YYYY-MM-DD"),
            timezone = "America/Los_Angeles"
        ) => ({
            start: moment(Kartra.utilities.date_pacific_time(start, timezone)).add(1, "days").startOf("day").valueOf(),
            end: moment(Kartra.utilities.date_pacific_time(end, timezone)).add(1, "days").endOf("day").valueOf(),
        }),

        rxreducer: rxreduce((prev, curr) => [...prev, ...curr]),

        queryDocs: (snapshot) => snapshot.docs.map((doc) => doc.data()),

        has_ad_id: anyPass([hasPath(["fb_ad_id"]), hasPath(["h_ad_id"]), hasPath(["fb_id"]), hasPath(["ad_id"])]),
    },

    orders: {
        get: ({ user_id, date }) => {
            let func_name = "Kartra:orders:get";
            console.log(func_name);

            if (!user_id) return throwError(`error:${func_name}:no user_id`);
            if (!date) return throwError(`error:${func_name}:no date`);

            let { start: start_timestamp, end: end_timestamp } = Kartra.utilities.date_start_end_timestamps(date);

            return from(
                getDocs(
                    query(
                        collection(db, "kartra"),
                        where("roas_user_id", "==", user_id),
                        where("created_at_unix_timestamp", ">", start_timestamp),
                        where("created_at_unix_timestamp", "<", end_timestamp)
                    )
                )
            ).pipe(
                rxmap((data) => data.docs.map((doc) => doc.data())),
                rxmap(get(matching({ action: "buy_product" }))),
                rxmap(mod(all)(({ lead, ...rest }) => ({ ...lead, ...rest }))),
                rxmap(mod(all)(pick(["first_name", "email", "ip", "roas_user_id", "action_details", "created_at_unix_timestamp", "last_name"]))),
                rxmap(
                    mod(
                        all,
                        "action_details",
                        "transaction_details"
                    )(
                        pick([
                            "lead_ip",
                            "lead_email",
                            "transaction_full_amount",
                            "gdpr_lead_status_ip",
                            "price",
                            "product_name",
                            "buyer_email",
                            "transaction_amount",
                            "transaction_base_amount",
                            "transaction_id",
                            "transaction_quantity",
                        ])
                    )
                ),
                rxmap(mod(all)(({ action_details, ...rest }) => ({ ...action_details.transaction_details, ...rest }))),
                defaultIfEmpty([])
            );
        },
    },

    order: {
        cart: {
            items: (order) => {
                let func_name = "Kartra:order:cart:items";
                console.log(func_name);

                return pipe(
                    get("line_items"),
                    mod(all)(({ transaction_full_amount, product_name }) => ({ price: Number(transaction_full_amount), name: product_name }))
                )(order);
            },
        },

        stats: {
            get: (order) => {
                let func_name = "Kartra:order:stats:get";
                console.log(func_name);

                let cart = Kartra.order.cart.items(order);
                return { roassales: size(cart), roasrevenue: pipe(get(all, "price"), sum)(cart) };
            },
        },

        ads: {
            ids: (order) => {
                let func_name = `Kartra:order:ads:get`;
                console.log(func_name);

                return Kartra.order.events.get(order).pipe(
                    concatMap(identity),
                    rxmap((event) => ({
                        ad_id: Event.ad.id(event),
                        timestamp: Math.trunc(Timestamp.toUTCDigit(Math.trunc(Event.get_utc_timestamp(event)))),
                    })),
                    rxmap(of),
                    rxreduce((prev, curr) => [...prev, ...curr]),
                    rxmap(lofilter((event) => !isUndefined(event.ad_id))),
                    rxfilter((ads) => !isEmpty(ads)),
                    rxmap(louniqby("ad_id")),
                    rxmap(loorderby(["timestamp"], ["desc"])),
                    defaultIfEmpty([])
                );
            },
        },

        events: {
            get: (order) => {
                let func_name = `Kartra:order:events:get`;
                console.log(func_name);

                let ipv4_events = Events.user.get.ipv4({ ip: order.ip_address, roas_user_id: order.roas_user_id });
                let ipv6_events = Events.user.get.ipv6({ ip: order.ip_address, roas_user_id: order.roas_user_id });
                return zip([ipv4_events, ipv6_events]).pipe(
                    rxmap(([ipv4, ipv6]) => [...ipv4, ...ipv6]),
                    defaultIfEmpty([])
                );
            },
        },
    },

    customer: {
        normalize: (orders) => {
            let func_name = "Kartra:customer:normalize";
            console.log(func_name);

            let lead_ip = pipe(get(all, "lead_ip"), head)(orders);
            let lead_email = pipe(get(all, "lead_email"), head)(orders);
            let transaction_full_amount = pipe(get(all, "transaction_full_amount"), head)(orders);
            let gdpr_lead_status_ip = pipe(get(all, "gdpr_lead_status_ip"), head)(orders);
            let buyer_email = pipe(get(all, "buyer_email"), head)(orders);
            let first_name = pipe(get(all, "first_name"), head)(orders);
            let customer_email = pipe(get(all, "email"), head)(orders);
            let ip = pipe(get(all, "ip"), head)(orders);
            let roas_user_id = pipe(get(all, "roas_user_id"), head)(orders);
            let last_name = pipe(get(all, "last_name"), head)(orders);

            let ip_address = pipe(uniq, head)([lead_ip, ip, gdpr_lead_status_ip]);
            let email = pipe(uniq, head)([lead_email, buyer_email, customer_email]);

            let line_items = pipe(
                mod(all)((order) => ({
                    price: pipe(get("price"))(order),
                    product_name: pipe(get("product_name"))(order),
                    transaction_amount: pipe(get("transaction_amount"))(order),
                    transaction_base_amount: pipe(get("transaction_base_amount"))(order),
                    transaction_id: pipe(get("transaction_id"))(order),
                    transaction_quantity: pipe(get("transaction_quantity"))(order),
                    created_at_unix_timestamp: pipe(get("created_at_unix_timestamp"))(order),
                    transaction_full_amount: pipe(get("transaction_full_amount"))(order),
                }))
            )(orders);

            let payload = {
                ip_address,
                email,
                lower_case_email: toLower(email),
                first_name,
                roas_user_id,
                last_name,
                line_items,
            };

            return payload;
        },
    },

    report: {
        get: ({ user_id, date, fb_ad_account_id }) => {
            let func_name = `Kartra:report:get`;
            console.log(func_name);

            let orders = Kartra.orders.get({ user_id, date }).pipe(
                rxmap(logroupby("email")),
                rxmap(mod(all)(Kartra.customer.normalize)),
                tap((customers) => {
                    pipe(values, get(all, "ip_address"), uniq)(customers);
                }),
                rxmap(values),
                concatMap(identity),
                rxmap((order) => ({
                    ...order,
                    cart: Kartra.order.cart.items(order),
                    stats: Kartra.order.stats.get(order),
                })),
                rxmap(of),
                Kartra.utilities.rxreducer
            );

            let customers_from_db_events = from(orders).pipe(
                concatMap(identity),
                concatMap((customer) => {
                    let { ip_address, roas_user_id } = customer;

                    return zip([from(ipEvents("ipv4", ip_address, roas_user_id)), from(ipEvents("ipv6", ip_address, roas_user_id))]).pipe(
                        rxmap(flatten),
                        tap((value) => console.log("size ->", size(value))),
                        rxmap(pipe(lofilter(Kartra.utilities.has_ad_id))),
                        tap((value) => console.log("size <- ", size(value))),
                        concatMap(identity),
                        rxmap((event) => ({
                            ad_id: pipe(
                                paths([["fb_ad_id"], ["h_ad_id"], ["fb_id"], ["ad_id"]]),
                                compact,
                                uniq,
                                reject((id) => id == "%7B%7Bad.id%7D%7D" || id == "{{ad.id}}"),
                                head
                            )(event),
                            timestamp: pipe(Event.get_utc_timestamp)(event),
                            ip: ip_address,
                        })),
                        rxmap(of),
                        Kartra.utilities.rxreducer,
                        rxmap(loorderby(["timesamp"], ["desc"])),
                        rxmap(get(matching({ ad_id: (id) => !isEmpty(id) }))),
                        rxmap(louniqby("ad_id")),
                        rxmap(louniqby("timestamp")),
                        rxmap((ads) => ({ ...customer, ads })),
                        rxmap(of)
                    );
                }),
                Kartra.utilities.rxreducer
            );

            // return customers_from_db_events;

            return customers_from_db_events.pipe(
                concatMap(identity),
                concatMap((order) => {
                    let { ads, email } = order;
                    let ad_ids = pipe(mod(all)(pick(["ad_id"])))(ads);

                    let ad_details = Facebook.ads.details.get({ ad_ids, fb_ad_account_id, user_id, date }).pipe(
                        rxfilter((ad) => !isUndefined(ad.asset_id)),
                        rxmap((ad) => ({
                            ...ad,
                            email,
                            // ipv4: pipe(get(matching({ ad_id: ad.ad_id }), "ipv4"), head)(ads),
                            // ipv6: pipe(get(matching({ ad_id: ad.ad_id }), "ipv6"), head)(ads),
                            // user_agent: pipe(get(matching({ ad_id: ad.ad_id }), "user_agent"), head)(ads),
                            timestamp: pipe(get(matching({ ad_id: ad.ad_id }), "timestamp"), head)(ads),
                        })),
                        rxmap(of),
                        rxreduce((prev, curr) => [...prev, ...curr]),
                        defaultIfEmpty([])
                    );

                    return from(ad_details).pipe(
                        rxmap((ads) => ({ ...order, ads, email })),
                        defaultIfEmpty({ ...order, ads: [], email })
                    );
                }),
                rxmap(pick(["email", "cart", "ads", "stats", "lower_case_email", "email"])),
                rxmap(of),
                rxreduce((prev, curr) => [...prev, ...curr]),
                rxmap(get(matching({ ads: (ads) => !isEmpty(ads) }))),
                rxmap(pipe(logroupby("lower_case_email"), mod(all)(mergeAll))),
                rxmap((customers) => ({ customers })),
                rxmap((customers) => ({
                    ...customers,
                    date,
                    user_id,
                })),
                catchError((error) => {
                    console.log("Keap:report:get:error");
                    console.log(error);
                    return rxof(error);
                }),
                defaultIfEmpty({ date, customers: {}, user_id })
            );
        },
    },
};

exports.Kartra = Kartra;

// let user_id = "0kkoxAk90oerq6eWdo9u6R713Cq1";

// let date = "2022-05-18";

// from(getDocs(query(collection(db, "projects"), where("roas_user_id", "==", user_id))))
//     .pipe(
//         rxmap(Kartra.utilities.queryDocs),
//         rxmap(lofilter((project) => project.shopping_cart_name !== undefined)),
//         rxmap(head),
//         concatMap((project) => {
//             return from(
//                 getDocs(query(collectionGroup(db, "integrations"), where("account_name", "==", "facebook"), where("user_id", "==", user_id)))
//             ).pipe(
//                 rxmap(Kartra.utilities.queryDocs),
//                 rxmap(head),
//                 rxmap((facebook) => ({ ...facebook, ...project }))
//             );
//         })
//     )
//     .subscribe((project) => {
//         console.log("project");
//         console.log(project);

//         let { roas_user_id: user_id, fb_ad_account_id, payment_processor_id, shopping_cart_id } = project;
//         let payload = { user_id, fb_ad_account_id, payment_processor_id, shopping_cart_id, date };

//         Kartra.report.get(payload).subscribe((result) => {
//             console.log("result");
//             pipeLog(result);
//         });
//     });

// from(getDocs(query(collection(db, "events"), where("roas_user_id", "==", roas_user_id), limit(1))))
//     .pipe(rxmap(Kartra.utilities.queryDocs))
//     .subscribe(pipeLog);

// from(getDocs(query(collection(db, "kartra"), where("roas_user_id", "==", roas_user_id), limit(1))))
//     .pipe(rxmap(Kartra.utilities.queryDocs))
//     .subscribe(pipeLog);
